namespace MBrace.SampleRuntime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.SampleRuntime.Actors

[<AutoSerializable(false)>]
type ResourceManager(factory : ResourceFactory, jobQueue : IJobQueue, logger : ICloudLogger, assemblyManager, resources : ResourceRegistry) =
    interface IRuntimeResourceManager with
        member x.AssemblyManager: IAssemblyManager = assemblyManager
        
        member x.CancellationEntryFactory: ICancellationEntryFactory = factory :> _
        
        member x.CurrentWorker: IWorkerRef = WorkerRef.LocalWorker :> _
        
        member x.GetAvailableWorkers(): Async<IWorkerRef []> = 
            failwith "Not implemented yet"
        
        member x.JobQueue: IJobQueue = jobQueue
        
        member x.Logger: ICloudLogger = logger
        
        member x.RequestCounter(initialValue: int): Async<ICloudCounter> = async {
            let! c = factory.RequestCounter initialValue
            return c :> ICloudCounter
        }
        
        member x.RequestResultAggregator(capacity: int): Async<IResultAggregator<'T>> =  async {
            let! c = factory.RequestResultAggregator<'T>(capacity)
            return c :> IResultAggregator<'T>
        }
        
        member x.RequestTaskCompletionSource(): Async<ICloudTaskCompletionSource<'T>> = async {
            let! c = factory.RequestTaskCompletionSource<'T> ()
            return c :> ICloudTaskCompletionSource<'T>
        }
        
        member x.ResourceRegistry: ResourceRegistry = resources