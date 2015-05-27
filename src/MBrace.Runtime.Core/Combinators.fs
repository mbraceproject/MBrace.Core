module internal MBrace.Runtime.Combinators

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

#nowarn "444"


let inline private withCancellationToken (cts : ICloudCancellationToken) (ctx : ExecutionContext) =
    { ctx with CancellationToken = cts }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> JobExecutionMonitor.ProtectAsync ctx (f ctx cont))

let runParallel (queue : IJobQueue) (resources : IRuntimeResourceManager) 
                (currentJob : Job) (computations : seq<#Cloud<'T> * IWorkerRef option>) =

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
            let currentCts = ctx.CancellationToken
            let faultPolicy = ctx.Resources.Resolve<FaultPolicy> ()
            let! childCts = resources.RequestCancellationToken(parents = [|currentCts|])
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
                |> Array.mapi (fun i (c,w) -> Job.Create currentJob.ProcessId currentJob.Dependencies childCts.Token faultPolicy (onSuccess i) onException onCancellation c, w)
                |> queue.BatchEnqueue
                    
            JobExecutionMonitor.TriggerCompletion ctx })

let runChoice (queue : IJobQueue) (resources : IRuntimeResourceManager) 
                (currentJob : Job) (computations : seq<#Cloud<'T option> * IWorkerRef option>) =

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
            let currentCts = ctx.CancellationToken
            let faultPolicy = ctx.Resources.Resolve<FaultPolicy> ()
            let! childCts = resources.RequestCancellationToken(parents = [|currentCts|])
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
                |> Array.map (fun (c,w) -> Job.Create currentJob.ProcessId currentJob.Dependencies childCts.Token faultPolicy onSuccess onException onCancellation c, w)
                |> queue.BatchEnqueue
                    
            JobExecutionMonitor.TriggerCompletion ctx })

let runStartAsCloudTask (queue : IJobQueue) (resources : IRuntimeResourceManager) (currentJob : Job) 
                        (faultPolicy:FaultPolicy) (token : ICloudCancellationToken option) 
                        (target : IWorkerRef option) (computation : Cloud<'T>) = async {
    let! cts = async {
        match token with
        | Some ct -> return! resources.RequestCancellationToken(parents = [|ct|])
        | None -> return! resources.RequestCancellationToken()
    }

    let! cell = resources.RequestResultCell<'T>()
    let setResult ctx r = 
        async {
            do! cell.SetResult r
            cts.Cancel()
            JobExecutionMonitor.TriggerCompletion ctx
        } |> JobExecutionMonitor.ProtectAsync ctx

    let scont ctx t = setResult ctx (Completed t)
    let econt ctx e = setResult ctx (Exception e)
    let ccont ctx c = setResult ctx (Cancelled c)

    let job = Job.Create currentJob.ProcessId currentJob.Dependencies cts.Token faultPolicy scont econt ccont computation
    do! queue.Enqueue(job, ?target = target)
    return cell
}