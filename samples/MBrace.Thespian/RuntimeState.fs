namespace MBrace.Thespian.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store

/// Runtime instance identifier
type RuntimeId = private { Id : string }
with
    interface IRuntimeId with
        member x.Id: string = x.Id

    override x.ToString() = x.Id

    /// Creates a new runtime instance
    static member Create() = { Id = mkUUID() }

/// Serializable MBrace.Thespian cluster state object.
/// Used for all interactions with the cluster
[<NoEquality; NoComparison; AutoSerializable(true)>]
type RuntimeState =
    {
        /// Unique runtime identifier object
        Id : RuntimeId
        /// Thespian address of the runtime state hosting process
        Address : string
        /// Resource factory instace used for instantiating resources
        /// in the cluster state hosting process
        ResourceFactory : ResourceFactory
        /// CloudValue provider instance used by runtime
        StoreCloudValueProvider : StoreCloudValueProvider
        /// Cluster worker monitor
        WorkerManager : WorkerManager
        /// Cloud task instance
        TaskManager : CloudTaskManager
        /// Cloud job queue instance
        JobQueue : IJobQueue
        /// Cloud logging instance
        CloudLogger  : ActorCloudLogger
        /// Misc resources appended to runtime state
        Resources : ResourceRegistry
        /// Vagabond assembly directory
        AssemblyDirectory : string
    }
with
    /// <summary>
    ///     Creates a cluster state object that is hosted in the local process
    /// </summary>
    /// <param name="localLogger">Local recipient logger instance.</param>
    /// <param name="fileStoreConfig">File store configuration.</param>
    /// <param name="assemblyDirectory">Assembly directory used in store.</param>
    /// <param name="cacheDirectory">CloudValue cache directory used in store.</param>
    /// <param name="miscResources">Misc resources passed to cloud workflows.</param>
    static member Create(localLogger : ISystemLogger, fileStoreConfig : CloudFileStoreConfiguration, 
                                ?assemblyDirectory : string, ?cacheDirectory : string, ?miscResources : ResourceRegistry) =

        let id = RuntimeId.Create()
        let resourceFactory = ResourceFactory.Create()
        let workerManager = WorkerManager.Create(localLogger)
        let taskManager = CloudTaskManager.Create()
        let jobQueue = JobQueue.Create(workerManager)
                
        let assemblyDirectory = defaultArg assemblyDirectory "vagabond"
        let cacheDirectory = defaultArg cacheDirectory "cloudValue"
        let storeCloudValueConfig = CloudFileStoreConfiguration.Create(fileStoreConfig.FileStore, defaultDirectory = cacheDirectory)
        let cloudValueProvider = 
            StoreCloudValueProvider.InitCloudValueProvider(storeCloudValueConfig, 
                                                            cacheFactory = (fun () -> Config.ObjectCache), 
//                                                            localFileStore = (fun () -> CloudFileStoreConfiguration.Create(Config.FileSystemStore, "cloudValueCache")),
                                                            shadowPersistObjects = true)

        let resources = resource {
            yield CloudAtomConfiguration.Create(new ActorAtomProvider(resourceFactory))
            yield CloudQueueConfiguration.Create(new ActorQueueProvider(resourceFactory))
            yield new ActorDictionaryProvider(id.Id, resourceFactory) :> ICloudDictionaryProvider
            yield new FsPicklerBinaryStoreSerializer() :> ISerializer
            yield cloudValueProvider :> ICloudValueProvider
            match miscResources with Some r -> yield! r | None -> ()
            yield fileStoreConfig
        }

        {
            Id = RuntimeId.Create()
            Address = Config.LocalAddress

            ResourceFactory = resourceFactory
            StoreCloudValueProvider = cloudValueProvider
            WorkerManager = workerManager
            TaskManager = taskManager
            CloudLogger = ActorCloudLogger.Create(localLogger)
            JobQueue = jobQueue
            Resources = resources
            AssemblyDirectory = assemblyDirectory
        }

    /// <summary>
    ///     Creates a IRuntimeManager instance using provided state
    ///     and given local logger instance.
    /// </summary>
    /// <param name="localLogger">Logger instance bound to local process.</param>
    member state.GetLocalRuntimeManager(localLogger : ISystemLogger) =
        new RuntimeManager(state, localLogger) :> IRuntimeManager

    /// <summary>
    ///     Serializes runtime state object to BASE64 string.
    ///     Used for quick-and-dirty CLI argument passing.
    /// </summary>
    member state.ToBase64 () =
        let pickle = Config.Serializer.Pickle(state)
        System.Convert.ToBase64String pickle

    /// <summary>
    ///     Deserializes runtime state object from BASE64 serialization.
    ///     Used for quick-and-dirty CLI argument passing.
    /// </summary>
    /// <param name="base64Text">Serialized state object in BASE64 format.</param>
    static member FromBase64(base64Text:string) =
        let bytes = System.Convert.FromBase64String(base64Text)
        Config.Serializer.UnPickle<RuntimeState> bytes

/// Local IRuntimeManager implementation
and [<AutoSerializable(false)>] private RuntimeManager (state : RuntimeState, logger : ISystemLogger) =

    let storeConfig = state.Resources.Resolve<CloudFileStoreConfiguration>()
    let serializer = state.Resources.Resolve<ISerializer>()
    let assemblyManager = StoreAssemblyManager.Create(storeConfig, serializer, state.AssemblyDirectory, logger = logger)
    // Install cache in the local application domain
    do state.StoreCloudValueProvider.Install()
    let cancellationEntryFactory = new ActorCancellationEntryFactory(state.ResourceFactory)
    let counterFactory = new ActorCounterFactory(state.ResourceFactory)
    let resultAggregatorFactory = new ActorResultAggregatorFactory(state.ResourceFactory)

    interface IRuntimeManager with
        member x.Id = state.Id :> _
        member x.Serializer = Config.Serializer :> _
        member x.AssemblyManager: IAssemblyManager = assemblyManager :> _
        
        member x.CancellationEntryFactory: ICancellationEntryFactory = cancellationEntryFactory :> _
        member x.CounterFactory: ICloudCounterFactory = counterFactory :> _
        member x.ResultAggregatorFactory: ICloudResultAggregatorFactory = resultAggregatorFactory :> _
        
        member x.WorkerManager = state.WorkerManager :> _
        
        member x.JobQueue: IJobQueue = state.JobQueue
        
        member x.GetCloudLogger (workerId : IWorkerId, job:CloudJob) : ICloudLogger = state.CloudLogger.GetCloudLogger(workerId, job)

        member x.SystemLogger : ISystemLogger = logger
        
        member x.TaskManager = state.TaskManager :> _
        
        member x.ResourceRegistry: ResourceRegistry = state.Resources

        member x.ResetClusterState () = async { return raise <| new System.NotImplementedException("cluster reset") }