namespace MBrace.Runtime.Store

open System
open System.Threading
open System.IO
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Vagabond

[<AutoOpen>]
module private StoreLoggerUtils =

    [<NoEquality; NoComparison>]
    type WriterMessage<'LogEntry> = 
        | Enqueue of 'LogEntry
        | FlushToStore of AsyncReplyChannel<exn option>

    let jsonLogSerializer = lazy(
        let jls = FsPickler.CreateJsonSerializer(indent = false, omitHeader = true, typeConverter = VagabondRegistry.Instance.TypeConverter)
        jls.UseCustomTopLevelSequenceSeparator <- true
        jls.SequenceSeparator <- System.Environment.NewLine
        jls)

    let writeLogs (store : ICloudFileStore) (path : string) (entries : seq<'LogEntry>) = async {
        use! stream = store.BeginWrite path
        ignore <| jsonLogSerializer.Value.SerializeSequence(stream, entries)
    }

    let readLogs<'LogEntry> (store : ICloudFileStore) (path : string) = async {
        let! stream = store.BeginRead path
        return jsonLogSerializer.Value.DeserializeSequence<'LogEntry>(stream, leaveOpen = false)
    }

    let readMultipleLogs<'LogEntry> (store : ICloudFileStore) (paths : string []) = async {
        let! entries = paths |> Seq.map (fun p -> readLogs<'LogEntry> store p) |> Async.Parallel
        return Seq.concat entries
    }

/// Defines an object that serializes log entries to underlying store in batches
[<Sealed; AutoSerializable(false)>]
type StoreJsonLogWriter<'LogEntry> internal (store : ICloudFileStore, nextLogFilePath : unit -> string, ?minInterval : int, ?maxInterval : int, ?minEntries : int, ?sysLogger : ISystemLogger) =

    let minInterval = defaultArg minInterval 100
    let maxInterval = defaultArg maxInterval 1000
    let minEntries  = defaultArg minEntries 5
    let sysLogger = match sysLogger with Some l -> l | None -> new NullLogger() :> _

    do if minInterval < 0 || maxInterval < minInterval then invalidArg "interval" "invalid intervals."

    let flush (entries : seq<'LogEntry>) = async {
        let path = nextLogFilePath()
        do! writeLogs store path entries
    }

    let cts = new CancellationTokenSource()
    let gatheredLogs = new ResizeArray<'LogEntry> ()

    let rec loop (mbox : MailboxProcessor<WriterMessage<'LogEntry>>) = async {
        let! msg = mbox.Receive()

        match msg with
        | Enqueue item -> gatheredLogs.Add item
        | FlushToStore ch -> 
            try
                if gatheredLogs.Count > 0 then
                    do! flush <| gatheredLogs.ToArray()
                    gatheredLogs.Clear()

                ch.Reply None

            with e ->
                ch.Reply <| Some e
                            
        return! loop mbox
    }

    let batch = new MailboxProcessor<_>(loop, cts.Token)

    let rec flusher interval = async {
        let sleepAndRecurseWith i = async {
            do! Async.Sleep minInterval
            return! flusher i
        }

        if interval > maxInterval || gatheredLogs.Count > minEntries then
            let! r = batch.PostAndAsyncReply FlushToStore
            match r with
            | None -> return! sleepAndRecurseWith 0
            | Some exn ->
                sysLogger.LogWithException LogLevel.Error exn "Error writing logs to store."
                return! sleepAndRecurseWith (interval + minInterval)

        else
            return! sleepAndRecurseWith (interval + minInterval)
    }

    do
        batch.Start()
        Async.Start(flusher 0, cts.Token)

    member self.LogEntry (entry : 'LogEntry) =
        batch.Post(Enqueue entry)

    member self.Flush () =
        match batch.PostAndReply FlushToStore with
        | None -> ()
        | Some e -> raise e

    interface IDisposable with
        member self.Dispose () = self.Flush () ; cts.Cancel()
            

/// Object used for polling log entries from cloud file store
[<Sealed; AutoSerializable(false)>]
type StoreJsonLogPoller<'LogEntry> internal (store : ICloudFileStore, getLogFiles : unit -> Async<string []>, ?pollingInterval : int) =

    let logsRead = new System.Collections.Generic.HashSet<string>()
    let cts = new CancellationTokenSource()

    let pollingInterval = defaultArg pollingInterval 500

    let updatedEvent = new Event<'LogEntry>()

    let updateLogs () = async {
        let! logFiles = getLogFiles ()
        let newFiles = logFiles |> Array.filter (not << logsRead.Contains)

        let! entries = readMultipleLogs<'LogEntry> store newFiles
        let _ = Async.StartAsTask(async { do for e in entries do updatedEvent.Trigger e})

        do newFiles |> Array.iter (logsRead.Add >> ignore)
    }

    let rec loop () = async {
        do! updateLogs ()
        do! Async.Sleep pollingInterval
        return! loop ()
    }

    do Async.Start(loop (), cancellationToken = cts.Token)

    [<CLIEvent>]
    member this.Updated = updatedEvent.Publish
    member this.Update () = Async.StartAsTask(updateLogs()) |> ignore

    interface IDisposable with
        member __.Dispose() = cts.Cancel()

type StoreJsonLogger =
    /// <summary>
    ///     Creates a logger implementation which asynchronously persists entries to underlying store using JSON.
    ///     Log files are persisted in batches which are created periodically depending on traffic.
    /// </summary>
    /// <param name="store">Underlying store to persist logs.</param>
    /// <param name="nextLogFilePath">User supplied, stateful function which returns successive log file paths.</param>
    /// <param name="minInterval">Minimum persist interval. Defaults to 100ms.</param>
    /// <param name="maxInterval">Maximum persist interval. Log entries guaranteed to be persisted within this interval. Defaults to 1000ms.</param>
    /// <param name="minEntries">Minimum number of entries to persist, if not reached max persist interval. Defaults to 5.</param>
    /// <param name="sysLogger">System logger used for logging errors of the writer itself. Defaults to no logging.</param>
    static member CreateJsonLogWriter<'LogEntry>(store : ICloudFileStore, nextLogFilePath : unit -> string, 
                                                    ?minInterval : int, ?maxInterval : int, ?minEntries : int, ?sysLogger : ISystemLogger) =

        new StoreJsonLogWriter<'LogEntry>(store, nextLogFilePath, ?sysLogger = sysLogger, 
                                            ?minInterval = minInterval, ?maxInterval = maxInterval, ?minEntries = minEntries)

    /// <summary>
    ///     Fetches all log entries found in store.
    /// </summary>
    /// <param name="store">Underlying store implementation.</param>
    /// <param name="logFiles">Log files to be read.</param>
    static member ReadJsonLogEntries<'LogEntry>(store : ICloudFileStore, logFiles : string []) : Async<seq<'LogEntry>> = async {
        return! readMultipleLogs<'LogEntry> store logFiles
    }

    /// <summary>
    ///     Creates an observable instance which polls the underlying store for new log entries.
    /// </summary>
    /// <param name="store">Underlying store to read logs from.</param>
    /// <param name="getLogFiles">User-supplied log file query implemenetation.</param>
    /// <param name="pollingInterval">Polling interval. Defaults to 500ms.</param>
    static member CreateJsonLogPoller<'LogEntry>(store : ICloudFileStore, getLogFiles : unit -> Async<string []>, ?pollingInterval : int) =
        new StoreJsonLogPoller<'LogEntry>(store, getLogFiles, ?pollingInterval = pollingInterval)


////////////////////////////////////////////////////////////

[<Sealed; AutoSerializable(false)>]
type private StoreCloudLogger (writer : StoreJsonLogWriter<CloudLogEntry>, job : CloudJob, workerId : string) =
    interface IJobLogger with
        member x.Dispose(): unit = (writer :> IDisposable).Dispose()
        member x.Log(message : string): unit = 
            // TODO : parameterize DateTime generation?
            let entry = new CloudLogEntry(job.TaskEntry.Id, workerId, job.Id, DateTime.Now, message)
            writer.LogEntry(entry) 

[<Sealed; AutoSerializable(false)>]
type private StoreCloudObservable (poller : StoreJsonLogPoller<CloudLogEntry>) =
    interface ILogObservable with
        member x.Dispose(): unit = (poller :> IDisposable).Dispose()
        member x.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable = poller.Updated.Subscribe observer

/// Cloud log manager implementation that uses the underlying cloud store for persisting and reading log entries.
[<Sealed; AutoSerializable(false)>]
type StoreCloudLogManager private (store : ICloudFileStore, ?getContainerByTaskId : string -> string, ?sysLogger : ISystemLogger) =
    let getContainerByTaskId =
        match getContainerByTaskId with
        | Some gcbt -> gcbt
        | None -> fun id -> sprintf "taskLogs-%s" id

    let getLogFile (job : CloudJob) (i : int) =
        let container = getContainerByTaskId job.TaskEntry.Id
        store.Combine(container, sprintf "job%s-%d.log" (job.Id.ToBase32String()) i)

    let enumerateTaskLogs (taskId : string) = async {
        let container = getContainerByTaskId taskId
        let! files = store.EnumerateFiles container
        return 
            files 
            |> Seq.filter (fun f -> f.EndsWith ".log")
            |> Seq.sort
            |> Seq.toArray
    }

    /// <summary>
    ///     Creates a new store log manager instance with supplied store and container parameters.
    /// </summary>
    /// <param name="store">Underlying store for cloud log manager.</param>
    /// <param name="getContainerByTaskId">User-supplied container generation function.</param>
    /// <param name="sysLogger">System logger. Defaults to no logging.</param>
    static member Create(store : ICloudFileStore, ?getContainerByTaskId : string -> string, ?sysLogger : ISystemLogger) =
        new StoreCloudLogManager(store, ?getContainerByTaskId = getContainerByTaskId, ?sysLogger = sysLogger)

    interface ICloudLogManager with
        member x.GetCloudLogger(worker: IWorkerId, job: CloudJob): Async<IJobLogger> = async {
            let logIdCounter = ref 0
            let nextLogFile() =
                let id = Interlocked.Increment logIdCounter
                getLogFile job id

            let writer = StoreJsonLogger.CreateJsonLogWriter<CloudLogEntry>(store, nextLogFile, ?sysLogger = sysLogger)
            return new StoreCloudLogger(writer, job, worker.Id) :> _
        }

        member x.GetAllCloudLogEntriesByTask(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! taskLogs = enumerateTaskLogs taskId
            return! StoreJsonLogger.ReadJsonLogEntries(store, taskLogs)
        }
        
        member x.GetCloudLogEntriesObservableByTask(taskId: string): Async<ILogObservable> = async {
            let poller = StoreJsonLogger.CreateJsonLogPoller(store, fun () -> enumerateTaskLogs taskId)
            return new StoreCloudObservable(poller) :> _
        }