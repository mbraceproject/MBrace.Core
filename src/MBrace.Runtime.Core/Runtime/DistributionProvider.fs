namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime

#nowarn "444"

/// Scheduling implementation provider
[<Sealed; AutoSerializable(false)>]
type DistributionProvider private (currentWorker : WorkerRef, runtime : IRuntimeManager, currentJob : CloudJob, faultPolicy : FaultPolicy, isForcedLocalParallelism : bool) =

    let logger = runtime.GetCloudLogger (currentWorker.WorkerId, currentJob)

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
        new DistributionProvider(currentWorker, runtime, job, job.FaultPolicy, false)
        
    interface IDistributionProvider with
        member __.ProcessId = currentJob.TaskEntry.Id
        member __.JobId = currentJob.Id

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy (newPolicy : FaultPolicy) =
            new DistributionProvider(currentWorker, runtime, currentJob, newPolicy, isForcedLocalParallelism) :> IDistributionProvider

        member __.CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken[]) = mkCts false parents

        member __.IsTargetedWorkerSupported = true
        member __.IsForcedLocalParallelismEnabled = isForcedLocalParallelism
        member __.WithForcedLocalParallelismSetting (setting : bool) = 
            new DistributionProvider(currentWorker, runtime, currentJob, faultPolicy, setting) :> IDistributionProvider

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts false, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts false, computations)

        member __.ScheduleParallel computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Parallel(mkNestedCts false, Seq.map fst computations)
            else
                return! Combinators.runParallel runtime currentJob.TaskEntry faultPolicy computations
        }

        member __.ScheduleChoice computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Choice(mkNestedCts false, Seq.map fst computations)
            else
                return! Combinators.runChoice runtime currentJob.TaskEntry faultPolicy computations
        }

        member __.ScheduleStartAsTask(workflow : Cloud<'T>, faultPolicy : FaultPolicy, ?cancellationToken : ICloudCancellationToken, ?target:IWorkerRef, ?taskName:string) = cloud {
            if isForcedLocalParallelism then
                return invalidOp <| sprintf "cannot initialize cloud task when evaluating using local semantics."
            else
                let! task = Combinators.runStartAsCloudTask runtime currentJob.TaskEntry.Info.Dependencies taskName faultPolicy cancellationToken target workflow 
                return task :> ICloudTask<'T>
        }

        member __.GetAvailableWorkers () = async {
            let! workerInfo = runtime.WorkerManager.GetAvailableWorkers()
            return workerInfo |> Array.map (fun i -> WorkerRef.Create(runtime, i.Id) :> IWorkerRef)
        }

        member __.CurrentWorker = currentWorker :> _
        member __.Logger = logger