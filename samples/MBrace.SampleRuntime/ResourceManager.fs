namespace MBrace.SampleRuntime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime
open MBrace.Runtime.Vagabond

[<AutoSerializable(false)>]
type ResourceManager(state : RuntimeState, logger : ISystemLogger) =
    let resources = resource {
        yield! state.Resources
        yield Config.ObjectCache
    }

    let storeConfig = resources.Resolve<CloudFileStoreConfiguration>()
    let serializer = resources.Resolve<ISerializer>()

    let assemblyManager = StoreAssemblyManager.Create(storeConfig, serializer, state.AssemblyDirectory, logger = logger)

    member __.State = state

    interface IRuntimeResourceManager with
        member x.AssemblyManager: IAssemblyManager = assemblyManager :> _
        
        member x.CancellationEntryFactory: ICancellationEntryFactory = state.Factory :> _
        
        member x.WorkerManager = state.WorkerManager :> _
        
        member x.JobQueue: IJobQueue = state.JobQueue
        
        member x.GetCloudLogger (job:CloudJob) : ICloudLogger = state.CloudLogger.CreateLogger(WorkerRef.LocalWorker, job)

        member x.SystemLogger : ISystemLogger = logger
        
        member x.RequestCounter(initialValue: int): Async<ICloudCounter> = async {
            let! c = state.Factory.RequestCounter initialValue
            return c :> ICloudCounter
        }
        
        member x.RequestResultAggregator(capacity: int): Async<IResultAggregator<'T>> =  async {
            let! c = state.Factory.RequestResultAggregator<'T>(capacity)
            return c :> IResultAggregator<'T>
        }
        
        member x.TaskManager = state.TaskManager :> _
        
        member x.ResourceRegistry: ResourceRegistry = resources

        member x.ResetClusterState () = async { return raise <| new System.NotImplementedException("cluster reset") }