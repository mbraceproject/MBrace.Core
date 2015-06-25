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

    member __.State = state

    interface IRuntimeManager with
        member x.Id = state.Id :> _
        member x.Serializer = Config.Serializer :> _
        member x.AssemblyManager: IAssemblyManager = assemblyManager :> _

        member x.PrimitivesFactory = state.Factory :> _
        
        member x.CancellationEntryFactory: ICancellationEntryFactory = state.Factory :> _
        
        member x.WorkerManager = state.WorkerManager :> _
        
        member x.JobQueue: IJobQueue = state.JobQueue
        
        member x.GetCloudLogger (workerId : IWorkerId, job:CloudJob) : ICloudLogger = state.CloudLogger.CreateLogger(workerId, job)

        member x.SystemLogger : ISystemLogger = logger
        
        member x.TaskManager = state.TaskManager :> _
        
        member x.ResourceRegistry: ResourceRegistry = resources

        member x.ResetClusterState () = async { return raise <| new System.NotImplementedException("cluster reset") }