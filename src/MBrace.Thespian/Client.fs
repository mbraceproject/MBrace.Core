﻿namespace MBrace.Thespian

open System
open System.IO
open System.Runtime.InteropServices

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils

open MBrace.Thespian.Runtime

/// A system logger that writes entries to stdout.
type ConsoleLogger = MBrace.Runtime.ConsoleLogger
/// Struct that specifies a single system log entry.
type SystemLogEntry = MBrace.Runtime.SystemLogEntry
/// Struct that specifies a single cloud log entry.
type CloudLogEntry = MBrace.Runtime.CloudLogEntry
/// Log level used by the MBrace runtime implementation.
type LogLevel = MBrace.Runtime.LogLevel
/// A Serializable object used to identify a specific worker in a cluster.
/// Can be used to point computations for execution at specific machines.
type WorkerRef = MBrace.Runtime.WorkerRef
/// Represents a distributed computation that is being executed by an MBrace runtime.
type CloudProcess = MBrace.Runtime.CloudProcess
/// Represents a distributed computation that is being executed by an MBrace runtime.
type CloudProcess<'T> = MBrace.Runtime.CloudProcess<'T>
/// Simple ICloudFileStore implementation using a local or shared File System.
type FileSystemStore = MBrace.Runtime.Store.FileSystemStore
/// FsPickler Binary Serializer implementation
type FsPicklerBinarySerializer = MBrace.Runtime.FsPicklerBinarySerializer
/// FsPickler Xml Serializer implementation
type FsPicklerXmlSerializer = MBrace.Runtime.FsPicklerXmlSerializer
/// FsPickler Json Serializer implementation
type FsPicklerJsonSerializer = MBrace.Runtime.FsPicklerJsonSerializer
/// Json.Net Serializer implementation
type JsonDotNetSerializer = MBrace.Runtime.JsonDotNetSerializer

/// Defines a client object used for administering MBrace worker processes.
[<AutoSerializable(false); StructuredFormatDisplay("{Uri}")>]
type ThespianWorker private (uri : string) =
    let protectAsync (w:Async<'T>) = async {
        try return! w
        with 
        | :? CommunicationException
        | :? UnknownRecipientException as e ->
            let msg = sprintf "Failed to communicate with mbrace worker at '%s'." uri
            return raise <| new Exception(msg, e)
    }

    let protect (f : unit -> 'T) =
        try f ()
        with 
        | :? CommunicationException
        | :? UnknownRecipientException as e ->
            let msg = sprintf "Failed to communicate with mbrace worker at '%s'." uri
            raise <| new Exception(msg, e)

    let aref = parseUri uri
    let state = CacheAtom.Create(protectAsync(async { return! aref <!- GetState }))
    do ignore state.Value

    static let isWindowsPlatform = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
    static let mutable executable = None

    /// Gets or sets a local path to the MBrace.Thespian worker executable.
    /// This is used for spawning of child worker processes from the client.
    static member LocalExecutable
        with get () = match executable with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            let path = Path.GetFullPath path
            if File.Exists path then executable <- Some path
            elif isWindowsPlatform && File.Exists (path + ".exe") then executable <- Some (path + ".exe")
            else raise <| FileNotFoundException(path)

    /// MBrace uri identifier for worker instance.
    member __.Uri = uri
    override __.ToString() = uri

    /// <summary>
    ///     Sends a kill signal to the worker process.
    /// </summary>
    /// <param name="signal">Process exit signal. Defaults to 1.</param>
    member __.Kill([<O;D(null:obj)>]?signal : int) : unit = 
        let signal = defaultArg signal 1
        protect(fun () -> aref <-- Kill signal)

    /// <summary>
    ///     Resets the cluster state of the worker process.
    /// </summary>
    member __.Reset() =
        protect(fun () -> aref <!= Reset)

    /// Gets whether worker process is idle.
    member __.IsIdle : bool =
        match state.Value with
        | None -> true
        | _ -> false
    
    /// Gets whether worker process acts as host (master node) to an MBrace cluster.
    member __.IsMasterNode : bool =
        match state.Value with
        | None -> false
        | Some(isMaster,_) -> isMaster

    /// Gets whether worker process is subscribed (slave node) to an MBrace cluster state.
    member __.IsWorkerNode : bool =
        match state.Value with
        | None -> false
        | Some(isMaster,_) -> not isMaster

    /// Gets the runtime state that the worker is participating, if applicable.
    member internal __.RuntimeState : ClusterState option =
        match state.Value with
        | None -> None
        | Some(_,state) -> Some state

    /// Initializes worker instance as master node in new cluster state.
    member internal __.InitAsClusterMasterNode(storeConfig : ICloudFileStore, [<O;D(null:obj)>]?miscResources : ResourceRegistry) = async {
        return! protectAsync (aref <!- fun ch -> InitMasterNode(storeConfig, miscResources, ch))
    }

    /// Initializes worker instance as slave node in supplied cluster state.
    member internal __.SubscribeToCluster(state : ClusterState) = async {
        return! protectAsync (aref <!- fun ch -> Subscribe(state, ch))
    }

    override __.Equals(other : obj) =
        match other with
        | :? ThespianWorker as w -> uri = w.Uri
        | _ -> false

    override __.GetHashCode() = uri.GetHashCode()

    /// <summary>
    ///     Connects to an MBrace worker process with supplied MBrace uri.
    /// </summary>
    /// <param name="uri">MBrace uri to worker.</param>
    static member Connect(uri : string) : ThespianWorker =
        new ThespianWorker(uri)

    /// <summary>
    ///     Gets an MBrace worker client instance from supplied WorkerRef object.
    /// </summary>
    /// <param name="worker">Input worker ref.</param>
    static member Connect(worker : IWorkerRef) : ThespianWorker =
        new ThespianWorker(worker.Id)

    /// <summary>
    ///     Asynchronously spawns a new worker process in the current machine with supplied configuration parameters.
    /// </summary>
    /// <param name="hostname">Hostname or IP address on which the worker will be listening to.</param>
    /// <param name="port">Master TCP port used by the worker. Defaults to self-assigned.</param>
    /// <param name="workingDirectory">Working directory used by the worker. Defaults to system temp folder.</param>
    /// <param name="maxConcurrentWorkItems">Maximum number of concurrent work items executed if running as slave node. Defaults to 20.</param>
    /// <param name="maxLogWriteInterval">Maximum interval in which new log entries are to be persisted to store. Defaults to 1 seconds.</param>
    /// <param name="logLevel">Loglevel used by the worker process. Defaults to no log level.</param>
    /// <param name="logFiles">Paths to text logfiles written to by worker process.</param>
    /// <param name="useAppDomainIsolation">Use AppDomain isolation when executing cloud work items. Defaults to true.</param>
    /// <param name="runAsBackground">Run as background process. Defaults to false.</param>
    /// <param name="quiet">Suppress logging to stdout. Defaults to false.</param>
    /// <param name="heartbeatInterval">Specifies the default heartbeat interval emitted by the worker. Defaults to 500ms</param>
    /// <param name="heartbeatThreshold">Specifies the maximum time threshold of heartbeats after which the worker will be declared dead. Defaults to 10sec.</param>
    static member SpawnAsync ([<O;D(null:obj)>]?hostname : string, [<O;D(null:obj)>]?port : int, [<O;D(null:obj)>]?workingDirectory : string, [<O;D(null:obj)>]?maxConcurrentWorkItems : int,
                                [<O;D(null:obj)>]?maxLogWriteInterval : TimeSpan,
                                [<O;D(null:obj)>]?logLevel : LogLevel, [<O;D(null:obj)>]?logFiles : seq<string>, [<O;D(null:obj)>]?useAppDomainIsolation : bool, [<O;D(null:obj)>]?runAsBackground : bool,
                                [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan) =
        async {
            let exe = ThespianWorker.LocalExecutable
            let logFiles = match logFiles with None -> [] | Some ls -> Seq.toList ls
            let runAsBackground = defaultArg runAsBackground false
            let config = 
                { MaxConcurrentWorkItems = maxConcurrentWorkItems ; UseAppDomainIsolation = useAppDomainIsolation ;
                    Hostname = hostname ; Port = port ; WorkingDirectory = workingDirectory ; MaxLogWriteInterval = maxLogWriteInterval ;
                    LogLevel = logLevel ; LogFiles = logFiles ; Parent = None ; Quiet = defaultArg quiet false ;
                    HeartbeatInterval = heartbeatInterval ; HeartbeatThreshold = heartbeatThreshold }

            let! ref = spawnAsync exe runAsBackground config
            return new ThespianWorker(mkUri ref)
        }

    /// <summary>
    ///     Spawns a new worker process in the current machine with supplied configuration parameters.
    /// </summary>
    /// <param name="hostname">Hostname or IP address on which the worker will be listening to.</param>
    /// <param name="port">Master TCP port used by the worker. Defaults to self-assigned.</param>
    /// <param name="workingDirectory">Working directory used by the worker. Defaults to system temp folder.</param>
    /// <param name="maxConcurrentWorkItems">Maximum number of concurrent work items executed if running as slave node. Defaults to 20.</param>
    /// <param name="maxLogWriteInterval">Maximum interval in which new log entries are to be persisted to store. Defaults to 1 seconds.</param>
    /// <param name="logLevel">Loglevel used by the worker process. Defaults to no log level.</param>
    /// <param name="logFiles">Paths to text logfiles written to by worker process.</param>
    /// <param name="useAppDomainIsolation">Use AppDomain isolation when executing cloud work items. Defaults to true.</param>
    /// <param name="runAsBackground">Run as background process. Defaults to false.</param>
    /// <param name="quiet">Suppress logging to stdout. Defaults to false.</param>
    /// <param name="heartbeatInterval">Specifies the default heartbeat interval emitted by the worker. Defaults to 500ms</param>
    /// <param name="heartbeatThreshold">Specifies the maximum time threshold of heartbeats after which the worker will be declared dead. Defaults to 10sec.</param>
    static member Spawn ([<O;D(null:obj)>]?hostname : string, [<O;D(null:obj)>]?port : int, [<O;D(null:obj)>]?workingDirectory : string, [<O;D(null:obj)>]?maxConcurrentWorkItems : int,
                            [<O;D(null:obj)>]?maxLogWriteInterval : TimeSpan,
                            [<O;D(null:obj)>]?logLevel : LogLevel, [<O;D(null:obj)>]?logFiles : seq<string>, [<O;D(null:obj)>]?useAppDomainIsolation : bool, [<O;D(null:obj)>]?runAsBackground : bool,
                            [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>]?heartbeatInterval : TimeSpan, [<O;D(null:obj)>]?heartbeatThreshold : TimeSpan) =

        ThespianWorker.SpawnAsync(?hostname = hostname, ?port = port, ?maxConcurrentWorkItems = maxConcurrentWorkItems, ?logLevel = logLevel, 
                                    ?maxLogWriteInterval = maxLogWriteInterval, ?workingDirectory = workingDirectory,
                                    ?logFiles = logFiles, ?runAsBackground = runAsBackground, ?useAppDomainIsolation = useAppDomainIsolation,
                                    ?quiet = quiet, ?heartbeatInterval = heartbeatInterval, ?heartbeatThreshold = heartbeatThreshold)
        |> Async.RunSync


/// MBrace.Thespian client object used to manage cluster and submit work items for computation.
[<AutoSerializable(false)>]
type ThespianCluster private (state : ClusterState, manager : IRuntimeManager, defaultFaultPolicy : FaultPolicy option) =
    inherit MBraceClient(manager, defaultArg defaultFaultPolicy FaultPolicy.NoRetry)

    static do Config.Initialize(isClient = true, populateDirs = true)
    static let initWorkers logLevel quiet (count : int) (target : ClusterState) = async {
        if count < 0 then invalidArg "workerCount" "must be non-negative."
        let quiet = defaultArg quiet Config.IsUnix
        let attachNewWorker _ = async {
            let! (node : ThespianWorker) = ThespianWorker.SpawnAsync(?logLevel = logLevel, quiet = quiet)
            do! node.SubscribeToCluster(target)
        }

        do! Array.init count attachNewWorker |> Async.Parallel |> Async.Ignore
    }

    static let getDefaultStore() = FileSystemStore.CreateRandomLocal() :> ICloudFileStore

    let masterNode =
        if state.IsWorkerHosted then Some <| ThespianWorker.Connect state.Uri
        else
            None

    let logger = manager.RuntimeSystemLogManager.CreateLogWriter(WorkerId.LocalInstance) |> Async.RunSync
    let _ = manager.LocalSystemLogManager.AttachLogger logger

    private new (state : ClusterState, logLevel : LogLevel option, defaultFaultPolicy : FaultPolicy option) = 
        let manager = state.GetLocalRuntimeManager()
        Actor.Logger <- manager.SystemLogger
        manager.LocalSystemLogManager.LogLevel <- defaultArg logLevel LogLevel.Info
        new ThespianCluster(state, manager, defaultFaultPolicy)

    /// Gets the the uri identifier of the process hosting the cluster.
    member __.Uri = state.Uri

    /// Cluster instance unique identifier
    member __.UUID = manager.Id

    /// Gets the MBrace worker object that host the cluster state, if available.
    member __.MasterNode : ThespianWorker option = masterNode

    /// Gets whether the given cluster is hosted by an MBrace worker object.
    member __.IsWorkerHosted = state.IsWorkerHosted

    /// <summary>
    ///     Spawns provided count of new local worker processes and subscibes them to the cluster.
    /// </summary>
    /// <param name="count">Number of workers to be spawned and appended.</param>
    /// <param name="quiet">Suppress logging to stdout. Defaults to false.</param>
    member __.AttachNewLocalWorkers (workerCount : int, [<O;D(null:obj)>]?logLevel : LogLevel, [<O;D(null:obj)>]?quiet : bool) =
        let _ = initWorkers logLevel quiet workerCount state |> Async.RunSync
        ()

    /// <summary>
    ///     Subscribe a given worker instance as slave to current cluster.
    /// </summary>
    /// <param name="worker">Worker to be attached.</param>
    member __.AttachWorker (worker : ThespianWorker) =
        worker.SubscribeToCluster(state) |> Async.RunSync

    /// <summary>
    ///     Detaches supplied worker from current cluster.
    /// </summary>
    /// <param name="worker">Worker to be detached.</param>
    member __.DetachWorker (worker : IWorkerRef) =
        let node = ThespianWorker.Connect worker
        node.Reset()

    /// <summary>
    ///     Sends a kill signal to supplied worker process.
    /// </summary>
    /// <param name="worker">Worker to be killed.</param>
    member __.KillWorker (worker : IWorkerRef) =
        let node = ThespianWorker.Connect worker
        node.Kill()

    /// Sends a kill signal to all worker processes currently subscribed to cluster.
    member __.KillAllWorkers () =
        base.Workers |> Array.Parallel.iter (fun w -> __.KillWorker w)

    /// <summary>
    ///     Initializes a new MBrace cluster running within the current machine.
    ///     Cluster state will be hosted in the current client process and workers nodes
    ///     are processes that will be spawned for this purpose.
    /// </summary>
    /// <param name="workerCount">Number of workers to spawn for cluster.</param>
    /// <param name="hostClusterStateOnCurrentProcess">
    ///     Hosts the cluster state in the client process, making all worker processes stateless. 
    ///     Otherwise a separate worker process will be spawned to carry the cluster state. Defaults to true.
    /// </param>
    /// <param name="fileStore">File store configuration to be used for cluster. Defaults to file system store in the temp folder.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="resources">Additional resources to be appended to the MBrace execution context.</param>
    /// <param name="quiet">Suppress logging to stdout. Defaults to false.</param>
    /// <param name="logger">Logger implementation to attach on client by default. Defaults to no logging.</param>
    /// <param name="logLevel">Sets the log level for the cluster. Defaults to LogLevel.Info.</param>
    static member InitOnCurrentMachine(workerCount : int, [<O;D(null:obj)>] ?hostClusterStateOnCurrentProcess : bool, [<O;D(null:obj)>] ?fileStore : ICloudFileStore, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy,
                                        [<O;D(null:obj)>] ?resources : ResourceRegistry, [<O;D(null:obj)>]?quiet : bool, [<O;D(null:obj)>] ?logger : ISystemLogger, [<O;D(null:obj)>] ?logLevel : LogLevel) : ThespianCluster =

        if workerCount < 0 then invalidArg "workerCount" "must be non-negative."
        let hostClusterStateOnCurrentProcess = defaultArg hostClusterStateOnCurrentProcess true
        let fileStore = match fileStore with Some c -> c | None -> getDefaultStore ()
        let state =
            if hostClusterStateOnCurrentProcess then
                ClusterState.Create(fileStore, isWorkerHosted = false, ?miscResources = resources)
            else
                let master = ThespianWorker.Spawn(?logLevel = logLevel)
                master.InitAsClusterMasterNode(fileStore, ?miscResources = resources) |> Async.RunSync

        let _ = initWorkers logLevel quiet workerCount state |> Async.RunSync
        let cluster = new ThespianCluster(state, logLevel, faultPolicy)
        logger |> Option.iter (fun l -> cluster.AttachLogger l |> ignore)
        cluster

    /// <summary>
    ///     Initializes a new cluster state that is hosted on an existing worker instance.
    /// </summary>
    /// <param name="target">Target MBrace worker to host the cluster state.</param>
    /// <param name="fileStore">File store configuration to be used for cluster. Defaults to file system store in the temp folder.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    /// <param name="miscResources">Additional resources to be appended to the MBrace execution context.</param>
    /// <param name="logLevel">Sets the log level for the client instance. Defaults to LogLevel.Info.</param>
    static member InitOnWorker(target : ThespianWorker, [<O;D(null:obj)>] ?fileStore : ICloudFileStore, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy,
                                    [<O;D(null:obj)>] ?miscResources : ResourceRegistry, [<O;D(null:obj)>] ?logLevel : LogLevel) : ThespianCluster =

        let fileStore = match fileStore with Some c -> c | None -> getDefaultStore ()
        let state = target.InitAsClusterMasterNode(fileStore, ?miscResources = miscResources) |> Async.RunSync
        new ThespianCluster(state, logLevel, faultPolicy)

    /// <summary>
    ///     Connects to the cluster instance that is active in supplied MBrace worker instance.
    /// </summary>
    /// <param name="worker">Worker instance to extract runtime state from.</param>
    /// <param name="logLevel">Sets the log level for the client instance. Defaults to LogLevel.Info.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    static member Connect(worker : ThespianWorker, [<O;D(null:obj)>] ?logLevel : LogLevel, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy) : ThespianCluster =
        match worker.RuntimeState with
        | None -> invalidOp <| sprintf "Worker '%s' is not part of an active cluster." worker.Uri
        | Some state -> new ThespianCluster(state, logLevel, faultPolicy)

    /// <summary>
    ///     Connects to the cluster instance that is identified by supplied MBrace uri.
    /// </summary>
    /// <param name="uri">MBrace uri to connect to.</param>
    /// <param name="logLevel">Sets the log level for the client instance. Defaults to LogLevel.Info.</param>
    /// <param name="faultPolicy">The default fault policy to be used by the cluster. Defaults to NoRetry.</param>
    static member Connect(uri : string, [<O;D(null:obj)>] ?logLevel : LogLevel, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy) : ThespianCluster = 
        ThespianCluster.Connect(ThespianWorker.Connect uri, ?logLevel = logLevel, ?faultPolicy = faultPolicy)

[<AutoOpen>]
module ClientExtensions =

    type IWorkerRef with
        /// Gets a worker management object from supplied worker ref.
        member w.WorkerManager = ThespianWorker.Connect(w)