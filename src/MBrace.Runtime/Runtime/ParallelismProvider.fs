namespace MBrace.Runtime

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.ThreadPool.Internals

#nowarn "444"

/// Implements the IDistribution provider implementation to be passed to MBrace workflow execution
[<Sealed; AutoSerializable(false)>]
type ParallelismProvider private (currentWorker : WorkerRef, runtime : IRuntimeManager, currentWorkItem : CloudWorkItem, faultPolicy : IFaultPolicy, logger : ICloudWorkItemLogger, isForcedLocalParallelism : bool) =

    let mkCts elevate (parents : ICloudCancellationToken[]) = async {
        let! dcts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, parents, elevate = elevate) 
        return dcts :> ICloudCancellationTokenSource
    }

    let mkNestedCts elevate parent = mkCts elevate [|parent|] |> Async.RunSync

    /// <summary>
    ///     Creates a distribution provider instance for given cloud work item.
    /// </summary>
    /// <param name="currentWorker">Worker ref instance identifying current worker.</param>
    /// <param name="runtime">Runtime resource manager.</param>
    /// <param name="workItem">Work item to be executed.</param>
    static member Create(currentWorker: IWorkerId, runtime: IRuntimeManager, workItem: CloudWorkItem) = async {
        let currentWorker = WorkerRef.Create(runtime, currentWorker)
        let! logger = runtime.CloudLogManager.CreateWorkItemLogger (currentWorker.WorkerId, workItem)
        return new ParallelismProvider(currentWorker, runtime, workItem, workItem.FaultPolicy, logger, false)
    }

    interface IDisposable with
        member __.Dispose () = logger.Dispose()
        
    interface IParallelismProvider with
        member __.CloudProcessId = currentWorkItem.ProcEntry.Id
        member __.WorkItemId = currentWorkItem.Id.ToString()

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy (newPolicy : IFaultPolicy) =
            new ParallelismProvider(currentWorker, runtime, currentWorkItem, newPolicy, logger, isForcedLocalParallelism) :> IParallelismProvider

        member __.CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken[]) = mkCts false parents

        member __.IsTargetedWorkerSupported = true
        member __.IsForcedLocalParallelismEnabled = isForcedLocalParallelism
        member __.WithForcedLocalParallelismSetting (setting : bool) = 
            new ParallelismProvider(currentWorker, runtime, currentWorkItem, faultPolicy, logger, setting) :> IParallelismProvider

        member __.ScheduleLocalParallel (computations : seq<Local<'T>>) = Combinators.Parallel(mkNestedCts false, MemoryEmulation.Shared, computations)
        member __.ScheduleLocalChoice (computations : seq<Local<'T option>>) = Combinators.Choice(mkNestedCts false, MemoryEmulation.Shared, computations)

        member __.ScheduleParallel (computations : seq<#Cloud<'T> * IWorkerRef option>) = cloud {
            if isForcedLocalParallelism then
                // force threadpool parallelism semantics
                return! Combinators.Parallel(mkNestedCts false, MemoryEmulation.Shared, Seq.map fst computations)
            else
                return! Combinators.runParallel runtime currentWorkItem.ProcEntry faultPolicy computations
        }

        member __.ScheduleChoice (computations : seq<#Cloud<'T option> * IWorkerRef option>) = cloud {
            if isForcedLocalParallelism then
                // force threadpool parallelism semantics
                return! Combinators.Choice(mkNestedCts false, MemoryEmulation.Shared, Seq.map fst computations)
            else
                return! Combinators.runChoice runtime currentWorkItem.ProcEntry faultPolicy computations
        }

        member __.ScheduleStartCloudProcess(workflow : Cloud<'T>, faultPolicy : IFaultPolicy, ?cancellationToken : ICloudCancellationToken, ?target:IWorkerRef, ?taskName:string) = cloud {
            if isForcedLocalParallelism then
                return invalidOp <| sprintf "cannot initialize cloud process when evaluating using local semantics."
            else
                let! task = Combinators.runStartJob runtime (Some currentWorkItem.ProcEntry) currentWorkItem.ProcEntry.Info.Dependencies taskName faultPolicy cancellationToken currentWorkItem.ProcEntry.Info.AdditionalResources target workflow 
                return task :> ICloudProcess<'T>
        }

        member __.GetAvailableWorkers () = async {
            let! workerInfo = runtime.WorkerManager.GetAvailableWorkers()
            return workerInfo |> Array.map (fun i -> WorkerRef.Create(runtime, i.Id) :> IWorkerRef)
        }

        member __.CurrentWorker = currentWorker :> _
        member __.Logger = logger :> _