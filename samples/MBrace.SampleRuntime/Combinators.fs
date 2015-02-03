module internal MBrace.SampleRuntime.Combinators

//
//  Provides distributed implementations for Cloud.Parallel, Cloud.Choice and Cloud.StartChild
//

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Tasks

#nowarn "444"

let inline private withCancellationToken (cts : DistributedCancellationTokenSource) (ctx : ExecutionContext) =
    let token = cts.GetLocalCancellationToken()
    { Resources = ctx.Resources.Register(cts) ; CancellationToken = token }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> TaskExecutionMonitor.ProtectAsync ctx (f ctx cont))
        
let Parallel (state : RuntimeState) procInfo dependencies fp (computations : seq<Cloud<'T> * IWorkerRef option>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [| |] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| (comp, None) |] ->
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource> ()
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource(parent = currentCts)
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
                    else // results pending, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            // Create tasks and enqueue
            computations
            |> Array.mapi (fun i (c,w) -> PickledTask.CreateTask procInfo dependencies childCts fp (onSuccess i) onException onCancellation w c)
            |> state.EnqueueTasks
                    
            TaskExecutionMonitor.TriggerCompletion ctx })

let Choice (state : RuntimeState) procInfo dependencies fp (computations : seq<Cloud<'T option> * IWorkerRef option>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| (comp, None) |] -> Cloud.StartWithContinuations(comp, cont, ctx)
        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let n = computations.Length // avoid capturing computation array in cont closures
            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource>()
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource currentCts
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
                            // workflow already cancelled, declare task completion
                            TaskExecutionMonitor.TriggerCompletion ctx
                    else
                        // 'None', increment completion latch
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = n then 
                            // is last task to complete with 'None', pass None to parent scont
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            // other tasks pending, declare task completion
                            TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else // cancellation already triggered by different party, declare task completed.
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            // create child tasks
            computations
            |> Array.mapi (fun i (c,w) -> PickledTask.CreateTask procInfo dependencies childCts fp onSuccess onException onCancellation w c)
            |> state.EnqueueTasks
                    
            TaskExecutionMonitor.TriggerCompletion ctx })


let StartChild (state : RuntimeState) procInfo dependencies fp worker (computation : Cloud<'T>) = cloud {
    let! cts = Cloud.GetResource<DistributedCancellationTokenSource> ()
    let! resultCell = Cloud.OfAsync <| state.StartAsCell procInfo dependencies cts fp worker computation
    return cloud { 
        let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
        return result.Value
    }
}