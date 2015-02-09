module internal MBrace.SampleRuntime.Combinators

//
//  Provides distributed implementations for Cloud.Parallel, Cloud.Choice and Cloud.StartChild
//

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Types

#nowarn "444"

let inline private withCancellationToken (cts : ICloudCancellationToken) (ctx : ExecutionContext) =
    { ctx with CancellationToken = cts }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> JobExecutionMonitor.ProtectAsync ctx (f ctx cont))
        
let Parallel (state : RuntimeState) procInfo dependencies fp (computations : seq<Cloud<'T> * IWorkerRef option>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [| |] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] ->
            let (comp, cont) = Config.Pickler.Clone (comp, cont)
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let currentCts = ctx.CancellationToken :?> DistributedCancellationTokenSource
            let childCts = DistributedCancellationTokenSource.CreateLinkedCancellationTokenSource(currentCts, forceElevation = true)
            let! resultAggregator = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
            let! cancellationLatch = state.ResourceFactory.RequestLatch(0)

            let onSuccess i ctx (t : 'T) = 
                async {
                    let! isCompleted = resultAggregator.SetResult(i, t)
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
            computations
            |> Array.mapi (fun i (c,w) -> PickledJob.Create procInfo dependencies childCts fp (onSuccess i) onException onCancellation w c)
            |> state.EnqueueJobs
                    
            JobExecutionMonitor.TriggerCompletion ctx })

let Choice (state : RuntimeState) procInfo dependencies fp (computations : seq<Cloud<'T option> * IWorkerRef option>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] -> 
            let (comp, cont) = Config.Pickler.Clone (comp, cont)
            Cloud.StartWithContinuations(comp, cont, ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let n = computations.Length // avoid capturing computation array in cont closures
            let currentCts = ctx.CancellationToken :?> DistributedCancellationTokenSource
            let childCts = DistributedCancellationTokenSource.CreateLinkedCancellationTokenSource(currentCts, forceElevation = true)
            let! completionLatch = state.ResourceFactory.RequestLatch(0)
            let! cancellationLatch = state.ResourceFactory.RequestLatch(0)

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
            computations
            |> Array.mapi (fun i (c,w) -> PickledJob.Create procInfo dependencies childCts fp onSuccess onException onCancellation w c)
            |> state.EnqueueJobs
                    
            JobExecutionMonitor.TriggerCompletion ctx })


let StartAsCloudTask (state : RuntimeState) procInfo dependencies (ct : ICloudCancellationToken) fp worker (computation : Cloud<'T>) = cloud {
    let dcts = ct :?> DistributedCancellationTokenSource
    return! Cloud.OfAsync <| state.StartAsTask procInfo dependencies dcts fp worker computation
}