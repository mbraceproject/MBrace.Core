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

    member this.Update () = Async.StartAsTask(updateLogs()) |> ignore

    interface ILogPoller<'LogEntry>

    interface IDisposable with
        member __.Dispose() = __.Stop()

    interface IEvent<'LogEntry> with
        member x.AddHandler(handler: Handler<'LogEntry>): unit =
            updatedEvent.Publish.AddHandler handler
        
        member x.RemoveHandler(handler: Handler<'LogEntry>): unit = 
            updatedEvent.Publish.RemoveHandler handler
        
        member x.Subscribe(observer: IObserver<'LogEntry>): IDisposable = 
            updatedEvent.Publish.Subscribe observer
        

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
//  Store System Log Implementations
////////////////////////////////////////////////////////////

[<Sealed; AutoSerializable(false)>]
type private StoreSystemLogWriter (workerId : string, writer : StoreJsonLogWriter<SystemLogEntry>) =
    interface ISystemLogger with
        member x.LogEntry(entry: SystemLogEntry): unit = 
            let updated = SystemLogEntry.WithWorkerId(workerId, entry)
            writer.LogEntry updated

/// Creates a schema for writing and fetching system log files for specific workers
type ISystemLogStoreSchema =

    /// <summary>
    ///     Creates a path to a log file for supplied WorkerId and incremental index.
    /// </summary>
    /// <param name="workerId">Worker identifier.</param>
    abstract GetLogFilePath : worker:IWorkerId -> string

    /// <summary>
    ///     Gets all log files that have been persisted to store by given task identifier.
    /// </summary>
    /// <param name="workerId">Worker identifier.</param>
    abstract GetWorkerLogFiles : workerId:IWorkerId -> Async<string []>

    /// <summary>
    ///     Gets all log files that have been persisted to store.
    /// </summary>
    abstract GetLogFiles : unit -> Async<string []>

/// As simple store log schema where each cloud task creates its own root directory
/// for storing logfiles; possibly not suitable for Azure where root directories are containers.
type DefaultStoreSystemLogSchema(store : ICloudFileStore, ?logDirectoryPrefix : string, ?logExtension : string, ?getLogDirectorySuffix : IWorkerId -> string) =
    let logDirectoryPrefix = defaultArg logDirectoryPrefix "systemLogs"
    let logExtension = defaultArg logExtension ".json"
    let getLogDirectorySuffix = defaultArg getLogDirectorySuffix (fun w -> w.Id.ToLower())

    let getLogDirectory (workerId : IWorkerId) =
        let suffix = getLogDirectorySuffix workerId
        sprintf "%s-%s" logDirectoryPrefix suffix

    let enumerateLogs(directory : string) = async {
        try 
            let! files = store.EnumerateFiles directory
            let logFiles = files |> Array.filter (fun f -> f.EndsWith logExtension)
            let! timeStamps = logFiles |> Seq.map (fun f -> store.GetLastModifiedTime(f, false)) |> Async.Parallel
            return Array.zip logFiles timeStamps
        with :? DirectoryNotFoundException -> return [||]
    }

    interface ISystemLogStoreSchema with
        member __.GetLogFilePath(workerId) =
            let logDir = getLogDirectory workerId
            let timeStamp = DateTime.UtcNow
            let format = timeStamp.ToString("yyyyMMddTHHmmssfffZ")
            store.Combine(logDir, sprintf "logs-%s%s" format logExtension) 

        member x.GetLogFiles() = async {
            let! logDirectories = store.EnumerateDirectories (store.GetRootDirectory())
            let! logFiles =
                logDirectories 
                |> Seq.filter (fun d -> let dn = store.GetFileName d in dn.StartsWith logDirectoryPrefix)
                |> Seq.map enumerateLogs
                |> Async.Parallel

            return
                logFiles
                |> Seq.concat
                |> Seq.sortBy snd
                |> Seq.map fst
                |> Seq.toArray
        }

        member x.GetWorkerLogFiles(workerId: IWorkerId) = async {
            let logDir = getLogDirectory workerId
            let! logFiles = enumerateLogs logDir
            return logFiles |> Seq.sortBy snd |> Seq.map fst |> Seq.toArray
        }

/// Tools for writing worker system logs to store.
[<Sealed; AutoSerializable(false)>]
type StoreSystemLogManager(schema : ISystemLogStoreSchema, store : ICloudFileStore) =

    let clearWorkerLogs(workerId : IWorkerId) = async {
        let! logFiles = schema.GetWorkerLogFiles workerId
        do! logFiles |> Seq.map (fun f -> store.DeleteFile f) |> Async.Parallel |> Async.Ignore
    }

    /// <summary>
    ///     Creates a logger implementation that asynchronously writes entries to blob store
    /// </summary>
    /// <param name="workerId">Worker identifier.</param>
    member x.CreateLogWriter(workerId : IWorkerId) = async {
        do! clearWorkerLogs workerId
        let writer = StoreJsonLogger.CreateJsonLogWriter(store, fun () -> schema.GetLogFilePath workerId)
        return new StoreSystemLogWriter(workerId.Id, writer) :> ISystemLogger
    }

    interface IRuntimeSystemLogManager with
        member x.CreateLogWriter(workerId : IWorkerId) : Async<ISystemLogger> = x.CreateLogWriter workerId
        
        member x.GetRuntimeLogs(): Async<seq<SystemLogEntry>> = async {
            let! logFiles = schema.GetLogFiles()
            return! StoreJsonLogger.ReadJsonLogEntries(store, logFiles)
        }
        
        member x.GetWorkerLogs(id: IWorkerId): Async<seq<SystemLogEntry>> = async {
            let! logFiles = schema.GetWorkerLogFiles(id)
            return! StoreJsonLogger.ReadJsonLogEntries(store, logFiles)
        }

        member x.CreateLogPoller(): Async<ILogPoller<SystemLogEntry>> = async {
            let poller = StoreJsonLogger.CreateJsonLogPoller(store, schema.GetLogFiles)
            return poller :> _
        }
        
        member x.CreateWorkerLogPoller(id: IWorkerId): Async<ILogPoller<SystemLogEntry>> = async {
            let poller = StoreJsonLogger.CreateJsonLogPoller(store, fun () -> schema.GetWorkerLogFiles id)
            return poller :> _
        }

        member x.ClearLogs(id: IWorkerId): Async<unit> = async {
            let! logFiles = schema.GetWorkerLogFiles id
            do! logFiles |> Seq.map (fun f -> store.DeleteFile f) |> Async.Parallel |> Async.Ignore
        }
        
        member x.ClearLogs(): Async<unit> = async {
            let! logFiles = schema.GetLogFiles()
            do! logFiles |> Seq.map (fun f -> store.DeleteFile f) |> Async.Parallel |> Async.Ignore
        }






////////////////////////////////////////////////////////////
//  Store Cloud Log Implementations
////////////////////////////////////////////////////////////

[<Sealed; AutoSerializable(false)>]
type private StoreCloudLogger (writer : StoreJsonLogWriter<CloudLogEntry>, workItem : CloudWorkItem, workerId : string) =
    interface ICloudWorkItemLogger with
        member x.Dispose() : unit = (writer :> IDisposable).Dispose()
        member x.Log(message : string) : unit =
            let entry = new CloudLogEntry(workItem.TaskEntry.Id, workerId, workItem.Id, DateTimeOffset.Now, message)
            writer.LogEntry(entry)

/// Creates a schema for writing and fetching log files for specific Cloud tasks
/// in StoreCloudLogManager instances
type ICloudLogStoreSchema =
    /// <summary>
    ///     Creates a path to a log file for supplied CloudWorkItem and incremental index.
    /// </summary>
    /// <param name="workItem">Work item that will be logged.</param>
    abstract GetLogFilePath : workItem:CloudWorkItem -> string

    /// <summary>
    ///     Gets all log files that have been persisted to store by given task identifier.
    /// </summary>
    /// <param name="taskId">Cloud task identifier.</param>
    abstract GetLogFilesByTask : taskId:string -> Async<string []>

/// As simple store log schema where each cloud task creates its own root directory
/// for storing logfiles; possibly not suitable for Azure where root directories are containers.
type DefaultStoreCloudLogSchema(store : ICloudFileStore) =
    let getTaskDir (taskId:string) = sprintf "taskLogs-%s" taskId

    interface ICloudLogStoreSchema with
        member x.GetLogFilePath(workItem: CloudWorkItem): string = 
            let timeStamp = DateTime.UtcNow
            let format = timeStamp.ToString("yyyyMMddTHHmmssfffZ")
            store.Combine(getTaskDir workItem.TaskEntry.Id, sprintf "workItemLog-%s-%s.json" (workItem.Id.ToBase32String()) format)
        
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
type StoreCloudLogManager private (store : ICloudFileStore, schema : ICloudLogStoreSchema, ?sysLogger : ISystemLogger) =

    /// <summary>
    ///     Creates a new store log manager instance with supplied store and container parameters.
    /// </summary>
    /// <param name="store">Underlying store for cloud log manager.</param>
    /// <param name="getContainerByTaskId">User-supplied container generation function.</param>
    /// <param name="sysLogger">System logger. Defaults to no logging.</param>
    static member Create(store : ICloudFileStore, schema : ICloudLogStoreSchema, ?sysLogger : ISystemLogger) =
        new StoreCloudLogManager(store, schema, ?sysLogger = sysLogger)

    interface ICloudLogManager with
        member x.CreateWorkItemLogger(worker: IWorkerId, workItem: CloudWorkItem): Async<ICloudWorkItemLogger> = async {
            let writer = StoreJsonLogger.CreateJsonLogWriter<CloudLogEntry>(store, (fun () -> schema.GetLogFilePath workItem), ?sysLogger = sysLogger)
            return new StoreCloudLogger(writer, workItem, worker.Id) :> _
        }

        member x.GetAllCloudLogsByTask(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! taskLogs = schema.GetLogFilesByTask taskId
            return! StoreJsonLogger.ReadJsonLogEntries(store, taskLogs)
        }
        
        member x.GetCloudLogPollerByTask(taskId: string): Async<ILogPoller<CloudLogEntry>> = async {
            return StoreJsonLogger.CreateJsonLogPoller(store, fun () -> schema.GetLogFilesByTask taskId) :> _
        }