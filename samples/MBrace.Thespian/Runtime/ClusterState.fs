namespace MBrace.Thespian.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components

/// Runtime instance identifier
type RuntimeId = private { Id : string }
with
    interface IRuntimeId with member x.Id = x.Id
    override x.ToString() = x.Id
    /// Creates a new unique runtime id instance
    static member Create() = { Id = mkUUID() }

/// Serializable MBrace.Thespian cluster state client object.
/// Used for all interactions with the cluster
[<NoEquality; NoComparison; AutoSerializable(true)>]
type ClusterState =
    {
        /// Unique cluster identifier object
        Id : RuntimeId
        /// Thespian address of the runtime state hosting process
        Uri : string
        /// Indicates that the runtime is hosted by an MBrace worker instance.
        IsWorkerHosted : bool
        /// Default serializer instance
        Serializer : ISerializer
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
        JobQueue : JobQueue
        /// Misc resources appended to runtime state
        Resources : ResourceRegistry
        /// Local node state factory instance
        LocalStateFactory : LocalStateFactory
    }
with
    /// <summary>
    ///     Creates a cluster state object that is hosted in the local process
    /// </summary>
    /// <param name="fileStore">File store instance used by cluster.</param>
    /// <param name="isWorkerHosted">Indicates that instance is hosted by worker instance.</param>
    /// <param name="userDataDirectory">Directory used for storing user data.</param>
    /// <param name="jobsDirectory">Directory used for persisting jobs in store.</param>
    /// <param name="assemblyDirectory">Assembly directory used in store.</param>
    /// <param name="cacheDirectory">CloudValue cache directory used in store.</param>
    /// <param name="miscResources">Misc resources passed to cloud workflows.</param>
    static member Create(fileStore : ICloudFileStore, isWorkerHosted : bool, 
                            ?userDataDirectory : string, ?jobsDirectory : string, 
                            ?assemblyDirectory : string, ?cacheDirectory : string, ?miscResources : ResourceRegistry) =

        let userDataDirectory = defaultArg userDataDirectory "userData"
        let assemblyDirectory = defaultArg assemblyDirectory "vagabond"
        let jobsDirectory = defaultArg jobsDirectory "mbrace-data"
        let cacheDirectory = defaultArg cacheDirectory "cloudValue"

        let serializer = new VagabondFsPicklerBinarySerializer()
        let cloudValueStore = fileStore.WithDefaultDirectory cacheDirectory
        let mkCacheInstance () = Config.ObjectCache
        let mkLocalCachingFileStore () = (Config.FileSystemStore :> ICloudFileStore).WithDefaultDirectory "cloudValueCache"
        let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(cloudValueStore, mkCacheInstance, (*mkLocalCachingFileStore,*) shadowPersistObjects = true)
        let persistedValueManager = PersistedValueManager.Create(fileStore.WithDefaultDirectory jobsDirectory, serializer = serializer, persistThreshold = 512L * 1024L)

        let localStateFactory = DomainLocal.Create(fun () ->
            let logger = AttacheableLogger.Create(makeAsynchronous = false)
            let vagabondStoreConfig = StoreAssemblyManagerConfiguration.Create(fileStore.WithDefaultDirectory assemblyDirectory, serializer, container = assemblyDirectory)
            let assemblyManager = StoreAssemblyManager.Create(vagabondStoreConfig, logger)
            let siftConfig = ClosureSiftConfiguration.Create(cloudValueProvider)
            let siftManager = ClosureSiftManager.Create(siftConfig, logger)
            let storeLogger = StoreCloudLogManager.Create(fileStore, sysLogger = logger)
            {
                Logger = logger
                PersistedValueManager = persistedValueManager
                AssemblyManager = assemblyManager
                SiftManager = siftManager
                CloudLogManager = storeLogger 
            })

        let id = RuntimeId.Create()
        let resourceFactory = ResourceFactory.Create()
        let workerManager = WorkerManager.Create(localStateFactory)
        let taskManager = CloudTaskManager.Create(localStateFactory)
        let jobQueue = JobQueue.Create(workerManager, localStateFactory)

        let resources = resource {
            yield new ActorAtomProvider(resourceFactory) :> ICloudAtomProvider
            yield new ActorQueueProvider(resourceFactory) :> ICloudQueueProvider
            yield new ActorDictionaryProvider(id.Id, resourceFactory) :> ICloudDictionaryProvider
            yield serializer :> ISerializer
            yield cloudValueProvider :> ICloudValueProvider
            match miscResources with Some r -> yield! r | None -> ()
            yield fileStore.WithDefaultDirectory userDataDirectory
        }

        {
            Id = RuntimeId.Create()
            IsWorkerHosted = isWorkerHosted
            Uri = Config.LocalMBraceUri
            Serializer = serializer :> ISerializer

            ResourceFactory = resourceFactory
            StoreCloudValueProvider = cloudValueProvider
            WorkerManager = workerManager
            TaskManager = taskManager
            JobQueue = jobQueue
            Resources = resources
            LocalStateFactory = localStateFactory
        }

    /// <summary>
    ///     Creates a IRuntimeManager instance using provided state
    ///     and given local logger instance.
    /// </summary>
    /// <param name="localLogger">Logger instance bound to local process.</param>
    member state.GetLocalRuntimeManager() =
        new RuntimeManager(state) :> IRuntimeManager

/// Local IRuntimeManager implementation
and [<AutoSerializable(false)>] private RuntimeManager (state : ClusterState) =
    // force initialization of local configuration in the current AppDomain
    let localState = state.LocalStateFactory.Value
    // Install cache in the local application domain
    do state.StoreCloudValueProvider.InstallCacheOnLocalAppDomain()
    let cancellationEntryFactory = new ActorCancellationEntryFactory(state.ResourceFactory)
    let counterFactory = new ActorCounterFactory(state.ResourceFactory)
    let resultAggregatorFactory = new ActorResultAggregatorFactory(state.ResourceFactory, state.LocalStateFactory)

    interface IRuntimeManager with
        member x.Id = state.Id :> _
        member x.Serializer = Config.Serializer :> _
        member x.AssemblyManager: IAssemblyManager = localState.AssemblyManager :> _
        member x.CloudLogManager : ICloudLogManager = localState.CloudLogManager :> _
        
        member x.CancellationEntryFactory: ICancellationEntryFactory = cancellationEntryFactory :> _
        member x.CounterFactory: ICloudCounterFactory = counterFactory :> _
        member x.ResultAggregatorFactory: ICloudResultAggregatorFactory = resultAggregatorFactory :> _
        
        member x.WorkerManager = state.WorkerManager :> _
        
        member x.JobQueue: ICloudJobQueue = state.JobQueue :> _

        member x.SystemLogger : ISystemLogger = localState.Logger :> _
        
        member x.AttachSystemLogger (l : ISystemLogger) = localState.Logger.AttachLogger l
        member x.LogLevel 
            with get () = localState.Logger.LogLevel
            and set l = localState.Logger.LogLevel <- l
        
        member x.TaskManager = state.TaskManager :> _
        
        member x.ResourceRegistry: ResourceRegistry = state.Resources

        member x.ResetClusterState () = async { return raise <| new System.NotImplementedException("cluster reset") }