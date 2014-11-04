module internal Nessos.MBrace.SampleRuntime.Combinators

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.SampleRuntime.Actors
open Nessos.MBrace.SampleRuntime.RuntimeTypes

#nowarn "444"

let inline private withCancellationToken (cts : DistributedCancellationTokenSource) (ctx : ExecutionContext) =
    let token = cts.GetLocalCancellationToken()
    { Resources = ctx.Resources.Register(cts) ; CancellationToken = token }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> TaskExecutionMonitor.ProtectAsync ctx (f ctx cont))
        
let Parallel (state : RuntimeState) deps (computations : seq<Cloud<'T>>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx e
        | Choice1Of2 [||] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] ->
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->

            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource> ()
            let! childCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = currentCts)
            let! resultAggregator = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
            let! cancellationLatch = state.ResourceFactory.RequestLatch(0)

            let onSuccess i ctx (t : 'T) = 
                async {
                    let! isCompleted = resultAggregator.SetResult(i, t)
                    if isCompleted then
                        let! results = resultAggregator.ToArray()
                        childCts.Cancel()
                        cont.Success (withCancellationToken currentCts ctx) results
                    else
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then
                        childCts.Cancel()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            try
                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask deps childCts (onSuccess i) onException onCancellation computations.[i]
            with e ->
                childCts.Cancel() ; return! Async.Reraise e
                    
            TaskExecutionMonitor.TriggerCompletion ctx })

let Choice (state : RuntimeState) deps (computations : seq<Cloud<'T option>>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx e
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] -> Cloud.StartWithContinuations(comp, cont, ctx)
        | Choice1Of2 computations ->

            let n = computations.Length // avoid capturing computation array in cont closures
            let currentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource>()
            let! childCts = state.CancellationTokenManager.RequestCancellationTokenSource currentCts
            let! completionLatch = state.ResourceFactory.RequestLatch(0)
            let! cancellationLatch = state.ResourceFactory.RequestLatch(0)

            let onSuccess ctx (topt : 'T option) =
                async {
                    if Option.isSome topt then
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then
                            childCts.Cancel ()
                            cont.Success (withCancellationToken currentCts ctx) topt
                        else
                            TaskExecutionMonitor.TriggerCompletion ctx
                    else
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = n then
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else
                        TaskExecutionMonitor.TriggerCompletion ctx
                } |> TaskExecutionMonitor.ProtectAsync ctx

            try
                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask deps childCts onSuccess onException onCancellation computations.[i]
            with e ->
                childCts.Cancel() ; return! Async.Reraise e
                    
            TaskExecutionMonitor.TriggerCompletion ctx })

// timeout?
let StartChild (state : RuntimeState) deps (computation : Cloud<'T>) = cloud {
    let! cts = Cloud.GetResource<DistributedCancellationTokenSource> ()
    let! resultCell = Cloud.OfAsync <| state.StartAsCell deps cts computation
    return cloud { 
        let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
        return result.Value
    }
}