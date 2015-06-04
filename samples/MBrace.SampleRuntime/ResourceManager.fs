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
        
        member x.CurrentWorker: IWorkerRef = WorkerRef.LocalWorker :> _
        
        member x.GetAvailableWorkers(): Async<IWorkerRef []> = async {
            let! workers = state.WorkerMonitor.GetAllWorkers()
            return workers |> Array.map fst
        }
        
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
        
        member x.RequestTaskCompletionSource(): Async<ICloudTaskCompletionSource<'T>> = async {
            let! c = state.Factory.RequestTaskCompletionSource<'T> ()
            return c :> ICloudTaskCompletionSource<'T>
        }
        
        member x.ResourceRegistry: ResourceRegistry = resources