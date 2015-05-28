module internal MBrace.Runtime.Combinators

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

#nowarn "444"

let inline private withCancellationToken (cts : ICloudCancellationToken) (ctx : ExecutionContext) =
    { ctx with CancellationToken = cts }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> JobExecutionMonitor.ProtectAsync ctx (f ctx cont))

/// <summary>
///     Defines a workflow that schedules provided cloud workflows for parallel computation.
/// </summary>
/// <param name="resources">Runtime resource object.</param>
/// <param name="currentJob">Current cloud job being executed.</param>
/// <param name="computations">Computations to be executed in parallel.</param>
let runParallel (resources : IRuntimeResourceManager) (currentJob : CloudJob) (computations : seq<#Cloud<'T> * IWorkerRef option>) : Cloud<'T []> =

    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [| |] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] ->
            let (comp, cont) = FsPickler.Clone ((comp, cont))
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let faultPolicy = ctx.Resources.Resolve<FaultPolicy> ()
            let currentCts = ctx.CancellationToken
            let! childCts = DistributedCancellationToken.Create(resources.CancellationEntryFactory, [|currentCts|], elevate = true)
            let! resultAggregator = resources.RequestResultAggregator<'T>(capacity = computations.Length)
            let! cancellationLatch = resources.RequestCounter(0)

            let onSuccess i ctx (t : 'T) = 
                async {
                    let! isCompleted = resultAggregator.SetResult(i, t, overwrite = true)
                    if isCompleted then 
                        // this is the last child callback, aggregate result and call parent continuation
                        let! results = resultAggregator.ToArray()
                        childCts.Cancel()
                        cont.Success (withCancellationToken currentCts ctx) results
                    else // results pending, declare job completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first job to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare job completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first job to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else // cancellation already triggered by different party, declare job completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            // Create jobs and enqueue
            do!
                computations
                |> Array.mapi (fun i (c,w) -> CloudJob.Create(currentJob.Dependencies, currentJob.ProcessId, currentJob.ParentTask, childCts, faultPolicy, onSuccess i, onException, onCancellation, c), w)
                |> resources.JobQueue.BatchEnqueue
                    
            JobExecutionMonitor.TriggerCompletion ctx })

/// <summary>
///     Defines a workflow that schedules provided nondeterministic cloud workflows for parallel computation.
/// </summary>
/// <param name="resources">Runtime resource object.</param>
/// <param name="currentJob">Current cloud job being executed.</param>
/// <param name="computations">Computations to be executed in parallel.</param>
let runChoice (resources : IRuntimeResourceManager) (currentJob : CloudJob) (computations : seq<#Cloud<'T option> * IWorkerRef option>) =

    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] -> 
            let (comp, cont) = FsPickler.Clone ((comp, cont))
            Cloud.StartWithContinuations(comp, cont, ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let n = computations.Length // avoid capturing computation array in continuation closures
            let faultPolicy = ctx.Resources.Resolve<FaultPolicy> ()
            let currentCts = ctx.CancellationToken
            let! childCts = DistributedCancellationToken.Create(resources.CancellationEntryFactory, [|currentCts|], elevate = true)
            let! completionLatch = resources.RequestCounter(0)
            let! cancellationLatch = resources.RequestCounter(0)

            let onSuccess ctx (topt : 'T option) =
                async {
                    if Option.isSome topt then // 'Some' result, attempt to complete workflow
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then 
                            // first child to initiate cancellation, grant access to parent scont
                            childCts.Cancel ()
                            cont.Success (withCancellationToken currentCts ctx) topt
                        else
                            // workflow already cancelled, declare job completion
                            JobExecutionMonitor.TriggerCompletion ctx
                    else
                        // 'None', increment completion latch
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = n then 
                            // is last job to complete with 'None', pass None to parent scont
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            // other jobs pending, declare job completion
                            JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first job to request workflow cancellation, grant access
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare job completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first job to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else // cancellation already triggered by different party, declare job completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            // create child jobs
            do!
                computations
                |> Array.map (fun (c,w) -> CloudJob.Create(currentJob.Dependencies, currentJob.ProcessId, currentJob.ParentTask, childCts, faultPolicy, onSuccess, onException, onCancellation, c), w)
                |> resources.JobQueue.BatchEnqueue
                    
            JobExecutionMonitor.TriggerCompletion ctx })

/// <summary>
///     Executes provided cloud workflow as a cloud task using the provided resources and parameters.
/// </summary>
/// <param name="resources">Runtime resource object.</param>
/// <param name="dependencies">Vagabond dependencies for computation.</param>
/// <param name="processId">Process id for computation.</param>
/// <param name="faultPolicy">Fault policy for computation.</param>
/// <param name="token">Optional cancellation token for computation.</param>
/// <param name="target">Optional target worker identifier.</param>
/// <param name="computation">Computation to be executed.</param>
let runStartAsCloudTask (resources : IRuntimeResourceManager) (dependencies : AssemblyId[]) (processId : string)
                        (faultPolicy:FaultPolicy) (token : ICloudCancellationToken option) 
                        (target : IWorkerRef option) (computation : Cloud<'T>) = async {
    let! cts = async {
        match token with
        | Some ct -> return! DistributedCancellationToken.Create(resources.CancellationEntryFactory, parents = [|ct|], elevate = true)
        | None -> return! DistributedCancellationToken.Create(resources.CancellationEntryFactory, elevate = true)
    }

    let! tcs = resources.RequestTaskCompletionSource<'T>()
    let setResult ctx f = 
        async {
            do! f
            cts.Cancel()
            JobExecutionMonitor.TriggerCompletion ctx
        } |> JobExecutionMonitor.ProtectAsync ctx

    let scont ctx t = setResult ctx (tcs.SetCompleted t)
    let econt ctx e = setResult ctx (tcs.SetException e)
    let ccont ctx c = setResult ctx (tcs.SetCancelled c)

    let job = CloudJob.Create (dependencies, processId, tcs, cts, faultPolicy, scont, econt, ccont, computation)
    do! resources.JobQueue.Enqueue(job, ?target = target)
    return tcs
}