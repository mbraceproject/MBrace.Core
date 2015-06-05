namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime

#nowarn "444"

/// Scheduling implementation provider
[<Sealed; AutoSerializable(false)>]
type DistributionProvider private (resources : IRuntimeResourceManager, currentJob : CloudJob, faultPolicy : FaultPolicy, isForcedLocalParallelism : bool) =

    let logger = resources.GetCloudLogger currentJob

    let mkCts elevate (parents : ICloudCancellationToken[]) = async {
        let! dcts = DistributedCancellationToken.Create(resources.CancellationEntryFactory, parents, elevate = elevate) 
        return dcts :> ICloudCancellationTokenSource
    }

    let mkNestedCts elevate parent = mkCts elevate [|parent|] |> Async.RunSync

    /// <summary>
    ///     Creates a distribution provider instance for given cloud job.
    /// </summary>
    /// <param name="resources">Runtime resource manager.</param>
    /// <param name="job">Job to be executed.</param>
    static member Create(resources : IRuntimeResourceManager, job : CloudJob) =
        new DistributionProvider(resources, job, job.FaultPolicy, false)
        
    interface IDistributionProvider with
        member __.ProcessId = currentJob.ProcessId
        member __.JobId = currentJob.JobId

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy (newPolicy : FaultPolicy) =
            new DistributionProvider(resources, currentJob, newPolicy, isForcedLocalParallelism) :> IDistributionProvider

        member __.CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken[]) = mkCts false parents

        member __.IsTargetedWorkerSupported = true
        member __.IsForcedLocalParallelismEnabled = isForcedLocalParallelism
        member __.WithForcedLocalParallelismSetting (setting : bool) = 
            new DistributionProvider(resources, currentJob, faultPolicy, setting) :> IDistributionProvider

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts false, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts false, computations)

        member __.ScheduleParallel computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Parallel(mkNestedCts false, Seq.map fst computations)
            else
                return! Combinators.runParallel resources currentJob faultPolicy computations
        }

        member __.ScheduleChoice computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Choice(mkNestedCts false, Seq.map fst computations)
            else
                return! Combinators.runChoice resources currentJob faultPolicy computations
        }

        member __.ScheduleStartAsTask(workflow : Cloud<'T>, faultPolicy : FaultPolicy, ?cancellationToken : ICloudCancellationToken, ?target:IWorkerRef) = cloud {
            if isForcedLocalParallelism then
                return invalidOp <| sprintf "cannot initialize cloud task when evaluating as local semantics."
            else
                let! tcs = Cloud.OfAsync <| Combinators.runStartAsCloudTask resources currentJob.Dependencies currentJob.ProcessId faultPolicy cancellationToken target workflow 
                return tcs.Task
        }

        member __.GetAvailableWorkers () = resources.GetAvailableWorkers()
        member __.CurrentWorker = resources.CurrentWorker
        member __.Logger = logger