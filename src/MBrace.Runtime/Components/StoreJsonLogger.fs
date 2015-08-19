namespace MBrace.Runtime.Components

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

    let lockObj = new obj()
    let logsRead = new System.Collections.Generic.HashSet<string>()
    let mutable cancellationTokenSource : CancellationTokenSource option = None

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

    member __.Start() =
        lock lockObj (fun () ->
            match cancellationTokenSource with
            | Some _ -> ()
            | None ->
                let cts = new CancellationTokenSource()
                Async.Start(loop(), cts.Token)
                cancellationTokenSource <- Some cts)

    member __.Stop () =
        lock lockObj (fun () ->
            match cancellationTokenSource with
            | None -> ()
            | Some cts ->
                cts.Cancel()
                cancellationTokenSource <- None)

    [<CLIEvent>]
    member this.Updated = updatedEvent.Publish
    member this.Update () = Async.StartAsTask(updateLogs()) |> ignore

    interface IDisposable with
        member __.Dispose() = __.Stop()

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
    interface ICloudJobLogger with
        member x.Dispose(): unit = (writer :> IDisposable).Dispose()
        member x.Log(message : string): unit = 
            // TODO : parameterize DateTime generation?
            let entry = new CloudLogEntry(job.TaskEntry.Id, workerId, job.Id, DateTime.Now, message)
            writer.LogEntry(entry) 

[<Sealed; AutoSerializable(false)>]
type private StoreCloudLogObservable (poller : StoreJsonLogPoller<CloudLogEntry>) =
    let lockObj = new obj()
    let mutable subscriptionCount = 0
    let incr() = 
        lock lockObj (fun () ->
            if subscriptionCount = 0 then poller.Start()
            subscriptionCount <- subscriptionCount + 1)

    let decr() =
        lock lockObj (fun () ->
            if subscriptionCount = 1 then poller.Stop()
            subscriptionCount <- subscriptionCount - 1)

    interface IObservable<CloudLogEntry> with
        member x.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable =
            let d = poller.Updated.Subscribe observer
            incr ()
            let isDisposed = ref 0
            { new IDisposable with 
                member __.Dispose() = 
                    if Interlocked.Increment isDisposed = 1 then 
                        d.Dispose() ; decr()
            }

/// Creates a schema for writing and fetching log files for specific Cloud tasks
/// in StoreCloudLogManager instances
type IStoreLogSchema =
    /// <summary>
    ///     Creates a path to a log file for supplied CloudJob and incremental index.
    /// </summary>
    /// <param name="job">Job that will be logged.</param>
    /// <param name="index>Incremental index of logfile to be created.</param>
    abstract GetLogFilePath : job:CloudJob * index:int -> string

    /// <summary>
    ///     Gets all log files that have been persisted to store by given task identifier.
    /// </summary>
    /// <param name="taskId">Cloud task identifier.</param>
    abstract GetLogFilesByTask : taskId:string -> Async<string []>

/// As simple store log schema where each cloud task creates its own root directory
/// for storing logfiles; possibly not suitable for Azure where root directories are containers.
type DefaultStoreLogSchema(store : ICloudFileStore) =
    let getTaskDir (taskId:string) = sprintf "taskLogs-%s" taskId
    interface IStoreLogSchema with
        member x.GetLogFilePath(job: CloudJob, index: int): string = 
            store.Combine(getTaskDir job.TaskEntry.Id, sprintf "job%s-%d.log" (job.Id.ToBase32String()) index)
        
        member x.GetLogFilesByTask(taskId: string): Async<string []> = async {
            let container = getTaskDir taskId
            let! logFiles = async {
                try return! store.EnumerateFiles container
                with :? DirectoryNotFoundException -> return [||]
            }

            return
                logFiles
                |> Seq.filter (fun f -> f.EndsWith ".log")
                |> Seq.sort
                |> Seq.toArray
        }
        

/// Cloud log manager implementation that uses the underlying cloud store for persisting and reading log entries.
[<Sealed; AutoSerializable(false)>]
type StoreCloudLogManager private (store : ICloudFileStore, schema : IStoreLogSchema, ?sysLogger : ISystemLogger) =

    /// <summary>
    ///     Creates a new store log manager instance with supplied store and container parameters.
    /// </summary>
    /// <param name="store">Underlying store for cloud log manager.</param>
    /// <param name="getContainerByTaskId">User-supplied container generation function.</param>
    /// <param name="sysLogger">System logger. Defaults to no logging.</param>
    static member Create(store : ICloudFileStore, schema : IStoreLogSchema, ?sysLogger : ISystemLogger) =
        new StoreCloudLogManager(store, schema, ?sysLogger = sysLogger)

    interface ICloudLogManager with
        member x.CreateJobLogger(worker: IWorkerId, job: CloudJob): Async<ICloudJobLogger> = async {
            let logIdCounter = ref 0
            let nextLogFile() =
                let id = Interlocked.Increment logIdCounter
                schema.GetLogFilePath(job, index = id)

            let writer = StoreJsonLogger.CreateJsonLogWriter<CloudLogEntry>(store, nextLogFile, ?sysLogger = sysLogger)
            return new StoreCloudLogger(writer, job, worker.Id) :> _
        }

        member x.GetAllCloudLogsByTask(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! taskLogs = schema.GetLogFilesByTask taskId
            return! StoreJsonLogger.ReadJsonLogEntries(store, taskLogs)
        }
        
        member x.GetCloudLogObservableByTask(taskId: string): Async<IObservable<CloudLogEntry>> = async {
            let poller = StoreJsonLogger.CreateJsonLogPoller(store, fun () -> schema.GetLogFilesByTask taskId)
            return new StoreCloudLogObservable(poller) :> _
        }