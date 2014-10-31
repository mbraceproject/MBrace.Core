module internal Nessos.MBrace.SampleRuntime.Combinators

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.SampleRuntime.Actors
open Nessos.MBrace.SampleRuntime.RuntimeTypes

#nowarn "444"

let inline private updateCts (cts : DistributedCancellationTokenSource) (ctx : ExecutionContext) =
    let token = cts.GetLocalCancellationToken()
    { Resources = ctx.Resources.Register(cts) ; CancellationToken = token }

let inline private guard ctx (f : unit -> 'T) =
    try f () |> Some with e -> TaskCompletionEvent.TriggerFault ctx e ; None

let private guardedFromContinuations F =
    Cloud.FromContinuations(fun ctx cont ->
        match (try F ctx cont ; None with e -> Some e) with
        | None -> ()
        | Some e -> TaskCompletionEvent.TriggerFault ctx e)
        
let scheduleParallel (state : RuntimeState) deps (computations : seq<Cloud<'T>>) =
    guardedFromContinuations(fun ctx cont ->
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx e
        | Choice1Of2 [||] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] ->
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->

            let parentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource> ()
            let childCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = parentCts)
            let results = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
            let cancellationLatch = state.ResourceFactory.RequestLatch(0)

            let onSuccess i ctx (t : 'T) =
                let completedResults = 
                    guard ctx (fun () ->
                        if results.SetResult(i, t) then Some <| results.ToArray ()
                        else None)

                match completedResults with
                | Some(Some results) -> cont.Success (updateCts parentCts ctx) results
                | Some None -> TaskCompletionEvent.TriggerCompletion ctx
                | None -> ()

            let onException ctx e =
                let isAcquiredCancellation =
                    guard ctx (fun () ->
                        if cancellationLatch.Increment() = 1 then
                            childCts.Cancel() ; true
                        else
                            false)

                match isAcquiredCancellation with
                | Some true -> cont.Exception (updateCts parentCts ctx) e
                | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                | None -> ()

            let onCancellation ctx c =
                let isAcquiredCancellation =
                    guard ctx (fun () ->
                        if cancellationLatch.Increment() = 1 then
                            childCts.Cancel () ; true
                        else
                            false)

                match isAcquiredCancellation with
                | Some true -> cont.Cancellation (updateCts parentCts ctx) c
                | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                | None -> ()

            for i = 0 to computations.Length - 1 do
                state.EnqueueTask deps childCts (onSuccess i) onException onCancellation computations.[i]
                    
            TaskCompletionEvent.TriggerCompletion ctx)

let scheduleChoice (state : RuntimeState) deps (computations : seq<Cloud<'T option>>) =
    Cloud.FromContinuations(fun ctx cont ->
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx e
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current task
        // note that this invalidates expected workflow semantics w.r.t. mutability.
        | Choice1Of2 [| comp |] -> Cloud.StartWithContinuations(comp, cont, ctx)
        | Choice1Of2 computations ->

            let n = computations.Length // avoid capturing computation array in cont closures
            let parentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource>()
            let childCts = state.CancellationTokenManager.RequestCancellationTokenSource parentCts
            let completionLatch = state.ResourceFactory.RequestLatch(0)
            let cancellationLatch = state.ResourceFactory.RequestLatch(0)

            let onSuccess ctx (topt : 'T option) =
                if Option.isSome topt then
                    let isAcquiredCancellation =
                        guard ctx (fun () ->
                            if cancellationLatch.Increment() = 1 then
                                childCts.Cancel () ; true
                            else
                                false)

                    match isAcquiredCancellation with
                    | Some true -> cont.Success (updateCts parentCts ctx) topt
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()
                else
                    let isCompleted = guard ctx (fun () -> completionLatch.Increment () = n)

                    match isCompleted with
                    | Some true -> cont.Success (updateCts parentCts ctx) None
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

            let onException ctx e =
                let isAcquiredCancellation =
                    guard ctx (fun () ->
                        if cancellationLatch.Increment() = 1 then
                            childCts.Cancel () ; true
                        else
                            false)

                match isAcquiredCancellation with
                | Some true -> cont.Exception (updateCts parentCts ctx) e
                | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                | None -> ()

            let onCancellation ctx c =
                let isAcquiredCancellation =
                    guard ctx (fun () ->
                        if cancellationLatch.Increment() = 1 then
                            childCts.Cancel () ; true
                        else
                            false)

                match isAcquiredCancellation with
                | Some true -> cont.Cancellation (updateCts parentCts ctx) c
                | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                | None -> ()

            for i = 0 to computations.Length - 1 do
                state.EnqueueTask deps childCts onSuccess onException onCancellation computations.[i]
                    
            TaskCompletionEvent.TriggerCompletion ctx)

// timeout?
let scheduleStartChild (state : RuntimeState) deps (computation : Cloud<'T>) = cloud {
    let! cts = Cloud.GetResource<DistributedCancellationTokenSource> ()
    let resultCell = state.StartAsCell deps cts computation
    return cloud { 
        let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
        return result.Value
    }
}