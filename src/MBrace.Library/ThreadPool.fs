namespace Nessos.MBrace.Library

#nowarn "444"

open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Continuation

[<AutoOpen>]
module private SchedulerInternals =

    type Latch (init : int) =
        [<VolatileField>]
        let mutable value = init

        member __.Increment() = Interlocked.Increment &value
        member __.Decrement() = Interlocked.Decrement &value
        member __.Value = value

    let mkLinkedCts (parent : CancellationToken) = CancellationTokenSource.CreateLinkedTokenSource [| parent |]

    let scheduleTask res ct sc ec cc wf =
        ThreadPool.QueueUserWorkItem(fun _ ->
            let ctx = { Resources = res ; CancellationToken = ct }
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx))
        |> ignore

/// Collection of context-less combinators for 
/// execution within local thread context.
type ThreadPool =

    /// <summary>
    ///     Provides a context-less Cloud.Parallel implementation
    ///     for execution within the thread pool.
    /// </summary>
    /// <param name="computations">Input computations</param>
    static member Parallel (computations : seq<Cloud<'T>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            | Choice1Of2 [||] -> cont.Success ctx [||]
            | Choice1Of2 [| comp |] ->
                let cont' = Continuation.map (fun t -> [| t |]) cont
                Cloud.StartWithContinuations(comp, cont')

            | Choice1Of2 computations ->                    
                let results = Array.zeroCreate<'T> computations.Length
                let innerCts = mkLinkedCts ctx.CancellationToken
                let exceptionLatch = new Latch(0)
                let completionLatch = new Latch(0)

                // success continuations of a completed parallel workflow
                // are passed the original context. This is not
                // a problem since execution takes place in-memory.

                let onSuccess i _ (t : 'T) =
                    results.[i] <- t
                    if completionLatch.Increment() = results.Length then
                        cont.Success ctx results

                let onException _ edi =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Exception ctx edi

                let onCancellation _ c =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Cancellation ctx c

                for i = 0 to computations.Length - 1 do
                    scheduleTask ctx.Resources innerCts.Token (onSuccess i) onException onCancellation computations.[i])

    /// <summary>
    ///     Provides a context-less Cloud.Choice implementation
    ///     for execution within the thread pool.
    /// </summary>
    /// <param name="computations">Input computations</param>
    static member Choice(computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            | Choice1Of2 [||] -> cont.Success ctx None
            | Choice1Of2 [| comp |] -> Cloud.StartWithContinuations(comp, cont, ctx)
            | Choice1Of2 computations ->
                let innerCts = mkLinkedCts ctx.CancellationToken
                let completionLatch = new Latch(0)
                let exceptionLatch = new Latch(0)

                // success continuations of a completed parallel workflow
                // are passed the original context. This is not
                // a problem since execution takes place in-memory.

                let onSuccess _ (topt : 'T option) =
                    if Option.isSome topt then
                        if exceptionLatch.Increment() = 1 then
                            cont.Success ctx topt
                    else
                        if completionLatch.Increment () = computations.Length then
                            cont.Success ctx None

                let onException _ edi =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Exception ctx edi

                let onCancellation _ cdi =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Cancellation ctx cdi

                for i = 0 to computations.Length - 1 do
                    scheduleTask ctx.Resources innerCts.Token onSuccess onException onCancellation computations.[i])


    /// <summary>
    ///     Provides a context-less Cloud.StartChild implementation
    ///     for execution within the thread pool.
    /// </summary>
    /// <param name="computation">Input computation.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    static member StartChild (computation : Cloud<'T>, ?timeoutMilliseconds) : Cloud<Cloud<'T>> = cloud {
        let! resource = Cloud.GetResourceRegistry()
        let asyncWorkflow = Cloud.ToAsync(computation, resources = resource)
        let! chWorkflow = Cloud.OfAsync <| Async.StartChild(asyncWorkflow, ?millisecondsTimeout = timeoutMilliseconds)
        return Cloud.OfAsync chWorkflow
    }