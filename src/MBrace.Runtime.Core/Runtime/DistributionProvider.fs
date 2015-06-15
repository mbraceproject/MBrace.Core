namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime

#nowarn "444"

/// Scheduling implementation provider
[<Sealed; AutoSerializable(false)>]
type DistributionProvider private (currentWorker : IWorkerRef, runtime : IRuntimeManager, currentJob : CloudJob, faultPolicy : FaultPolicy, isForcedLocalParallelism : bool) =

    let logger = runtime.GetCloudLogger currentJob

    let mkCts elevate (parents : ICloudCancellationToken[]) = async {
        let! dcts = DistributedCancellationToken.Create(runtime.CancellationEntryFactory, parents, elevate = elevate) 
        return dcts :> ICloudCancellationTokenSource
    }

    let mkNestedCts elevate parent = mkCts elevate [|parent|] |> Async.RunSync

    /// <summary>
    ///     Creates a distribution provider instance for given cloud job.
    /// </summary>
    /// <param name="currentWorker">Worker ref instance identifying current worker.</param>
    /// <param name="resources">Runtime resource manager.</param>
    /// <param name="job">Job to be executed.</param>
    static member Create(currentWorker : IWorkerRef, resources : IRuntimeManager, job : CloudJob) =
        new DistributionProvider(currentWorker, resources, job, job.FaultPolicy, false)
        
    interface IDistributionProvider with
        member __.ProcessId = currentJob.TaskInfo.Id
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
                return! Combinators.runParallel runtime currentJob.TaskInfo faultPolicy computations
        }

        member __.ScheduleChoice computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Choice(mkNestedCts false, Seq.map fst computations)
            else
                return! Combinators.runChoice runtime currentJob.TaskInfo faultPolicy computations
        }

        member __.ScheduleStartAsTask(workflow : Cloud<'T>, faultPolicy : FaultPolicy, ?cancellationToken : ICloudCancellationToken, ?target:IWorkerRef, ?taskName:string) = cloud {
            if isForcedLocalParallelism then
                return invalidOp <| sprintf "cannot initialize cloud task when evaluating as local semantics."
            else
                let! tcs = Combinators.runStartAsCloudTask runtime currentJob.TaskInfo.Dependencies taskName faultPolicy cancellationToken target workflow 
                return tcs.Task
        }

        member __.GetAvailableWorkers () = async {
            let! workers = runtime.WorkerManager.GetAvailableWorkers()
            return workers |> Array.map (fun (w,_,_) -> w)
        }

        member __.CurrentWorker = currentWorker
        member __.Logger = logger