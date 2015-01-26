namespace MBrace.Runtime.InMemory

#nowarn "444"

open System.Threading

open MBrace
open MBrace.Continuation

/// Collection of workflows that emulate execute
/// the parallelism primitives sequentially.
[<CompilerMessage("Use of this API restricted to runtime implementers.", 444)>]
type Sequential =

    /// <summary>
    ///     A Cloud.Parallel implementation executed sequentially.
    /// </summary>
    /// <param name="computations">Input computations.</param>
    [<CompilerMessage("Use of Sequential.Parallel restricted to runtime implementers.", 444)>]
    static member Parallel(computations : seq<Cloud<'T>>) : Cloud<'T []> = cloud {
        let arr = ResizeArray<'T> ()
        for comp in Seq.toArray computations do
            let! r = comp in arr.Add r
        return arr.ToArray()
    }

    /// <summary>
    ///     A Cloud.Choice implementation executed sequentially.
    /// </summary>
    /// <param name="computations">Input computations.</param>
    [<CompilerMessage("Use of Sequential.Choice restricted to runtime implementers.", 444)>]
    static member Choice(computations : seq<Cloud<'T option>>) : Cloud<'T option> = cloud {
        let computations = Seq.toArray computations
        let rec aux i = cloud {
            if i = computations.Length then return None
            else
                let! r = computations.[i]
                match r with
                | None -> return! aux (i+1)
                | Some _ -> return r
        }

        return! aux 0
    }

    /// <summary>
    ///     Sequential Cloud.StartChild implementation.
    /// </summary>
    /// <param name="computation">Input computation.</param>
    [<CompilerMessage("Use of Sequential.StartChild restricted to runtime implementers.", 444)>]
    static member StartChild (computation : Cloud<'T>) = cloud {
        let! result = computation |> Cloud.Catch
        return cloud {  
            match result with 
            | Choice1Of2 t -> return t
            | Choice2Of2 e -> return! Cloud.Raise e
        }
    }

/// Collection of workflows that provide parallelism
/// using the .NET thread pool
type ThreadPool private () =

    static let mkLinkedCts (parent : CancellationToken) = CancellationTokenSource.CreateLinkedTokenSource [| parent |]

    static let scheduleTask res ct sc ec cc wf =
        Trampoline.QueueWorkItem(fun () ->
            let ctx = { Resources = res ; CancellationToken = ct }
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx))

    /// <summary>
    ///     A Cloud.Parallel implementation executed using the thread pool.
    /// </summary>
    /// <param name="computations">Input computations.</param>
    [<CompilerMessage("Use of ThreadPool.Parallel restricted to runtime implementers.", 444)>]
    static member Parallel (computations : seq<Cloud<'T>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            | Choice1Of2 [||] -> cont.Success ctx [||]
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] ->
                let cont' = Continuation.map (fun t -> [| t |]) cont
                Cloud.StartWithContinuations(comp, cont', ctx)

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
    ///     A Cloud.Choice implementation executed using the thread pool.
    /// </summary>
    /// <param name="computations">Input computations.</param>
    [<CompilerMessage("Use of ThreadPool.Choice restricted to runtime implementers.", 444)>]
    static member Choice(computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            | Choice1Of2 [||] -> cont.Success ctx None
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] -> Cloud.StartWithContinuations(comp, cont, ctx)
            | Choice1Of2 computations ->
                let innerCts = mkLinkedCts ctx.CancellationToken
                let completionLatch = new Latch(0)
                let exceptionLatch = new Latch(0)

                // success continuations of a completed parallel workflow
                // are passed the original context, which is not serializable. 
                // This is not a problem since execution takes place in-memory.

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
    ///     A Cloud.StartChild implementation executed using the thread pool.
    /// </summary>
    /// <param name="computation">Input computation.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    [<CompilerMessage("Use of ThreadPool.StartChild restricted to runtime implementers.", 444)>]
    static member StartChild (computation : Cloud<'T>, ?timeoutMilliseconds) : Cloud<Cloud<'T>> = cloud {
        let! resource = Cloud.GetResourceRegistry()
        let asyncWorkflow = Cloud.ToAsync(computation, resources = resource)
        let! chWorkflow = Cloud.OfAsync <| Async.StartChild(asyncWorkflow, ?millisecondsTimeout = timeoutMilliseconds)
        return Cloud.OfAsync chWorkflow
    }