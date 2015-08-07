namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.InMemoryRuntime

#nowarn "444"

/// Implements the IDistribution provider implementation to be passed to MBrace workflow execution
[<Sealed; AutoSerializable(false)>]
type DistributionProvider private (currentWorker : WorkerRef, runtime : IRuntimeManager, currentJob : CloudJob, faultPolicy : FaultPolicy, logger : ICloudLogger, isForcedLocalParallelism : bool) =

    let mkCts elevate (parents : ICloudCancellationToken[]) = async {
        let! dcts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, parents, elevate = elevate) 
        return dcts :> ICloudCancellationTokenSource
    }

    let mkNestedCts elevate parent = mkCts elevate [|parent|] |> Async.RunSync

    /// <summary>
    ///     Creates a distribution provider instance for given cloud job.
    /// </summary>
    /// <param name="currentWorker">Worker ref instance identifying current worker.</param>
    /// <param name="runtime">Runtime resource manager.</param>
    /// <param name="job">Job to be executed.</param>
    static member Create(currentWorker : IWorkerId, runtime : IRuntimeManager, job : CloudJob) =
        let currentWorker = WorkerRef.Create(runtime, currentWorker)
        let logger = runtime.GetCloudLogger (currentWorker.WorkerId, job)
        new DistributionProvider(currentWorker, runtime, job, job.FaultPolicy, logger, false)
        
    interface IParallelismProvider with
        member __.ProcessId = currentJob.TaskEntry.Id
        member __.JobId = currentJob.Id

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy (newPolicy : FaultPolicy) =
            new DistributionProvider(currentWorker, runtime, currentJob, newPolicy, logger, isForcedLocalParallelism) :> IParallelismProvider

        member __.CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken[]) = mkCts false parents

        member __.IsTargetedWorkerSupported = true
        member __.IsForcedLocalParallelismEnabled = isForcedLocalParallelism
        member __.WithForcedLocalParallelismSetting (setting : bool) = 
            new DistributionProvider(currentWorker, runtime, currentJob, faultPolicy, logger, setting) :> IParallelismProvider

        member __.ScheduleLocalParallel (computations : seq<Local<'T>>) = ThreadPool.Parallel(mkNestedCts false, MemoryEmulation.Shared, computations)
        member __.ScheduleLocalChoice (computations : seq<Local<'T option>>) = ThreadPool.Choice(mkNestedCts false, MemoryEmulation.Shared, computations)

        member __.ScheduleParallel (computations : seq<#Cloud<'T> * IWorkerRef option>) = cloud {
            if isForcedLocalParallelism then
                // force threadpool parallelism semantics
                return! ThreadPool.Parallel(mkNestedCts false, MemoryEmulation.Shared, Seq.map fst computations)
            else
                return! Combinators.runParallel runtime currentJob.TaskEntry faultPolicy computations
        }

        member __.ScheduleChoice (computations : seq<#Cloud<'T option> * IWorkerRef option>) = cloud {
            if isForcedLocalParallelism then
                // force threadpool parallelism semantics
                return! ThreadPool.Choice(mkNestedCts false, MemoryEmulation.Shared, Seq.map fst computations)
            else
                return! Combinators.runChoice runtime currentJob.TaskEntry faultPolicy computations
        }

        member __.ScheduleStartAsTask(workflow : Cloud<'T>, faultPolicy : FaultPolicy, ?cancellationToken : ICloudCancellationToken, ?target:IWorkerRef, ?taskName:string) = cloud {
            if isForcedLocalParallelism then
                return invalidOp <| sprintf "cannot initialize cloud task when evaluating using local semantics."
            else
                let! task = Combinators.runStartAsCloudTask runtime false currentJob.TaskEntry.Info.Dependencies taskName faultPolicy cancellationToken target workflow 
                return task :> ICloudTask<'T>
        }

        member __.GetAvailableWorkers () = async {
            let! workerInfo = runtime.WorkerManager.GetAvailableWorkers()
            return workerInfo |> Array.map (fun i -> WorkerRef.Create(runtime, i.Id) :> IWorkerRef)
        }

        member __.CurrentWorker = currentWorker :> _
        member __.Logger = logger