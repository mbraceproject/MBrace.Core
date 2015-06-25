namespace MBrace.Thespian.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime
open MBrace.Runtime.Vagabond

[<AutoSerializable(false)>]
type RuntimeManager(state : RuntimeState, logger : ISystemLogger) =
    let resources = resource {
        yield! state.Resources
        yield Config.ObjectCache
    }

    let storeConfig = resources.Resolve<CloudFileStoreConfiguration>()
    let serializer = resources.Resolve<ISerializer>()

    let assemblyManager = StoreAssemblyManager.Create(storeConfig, serializer, state.AssemblyDirectory, logger = logger)
    let cancellationEntryFactory = new ActorCancellationEntryFactory(state.Factory)
    let counterFactory = new ActorCounterFactory(state.Factory)
    let resultAggregatorFactory = new ActorResultAggregatorFactory(state.Factory)

    member __.State = state

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
        
        member x.ResourceRegistry: ResourceRegistry = resources

        member x.ResetClusterState () = async { return raise <| new System.NotImplementedException("cluster reset") }