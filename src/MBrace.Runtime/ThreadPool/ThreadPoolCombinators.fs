namespace MBrace.ThreadPool.Internals

#nowarn "444"

open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.ThreadPool


/// Concurrent counter implementation
[<AutoSerializable(false); CloneableOnly>]
type private ConcurrentCounter (init : int) =
    [<VolatileField>]
    let mutable value = init
    /// Increments counter by 1
    member __.Increment() = Interlocked.Increment &value
    /// Gets the current counter value
    member __.Value = value

[<AutoSerializable(false); CloneableOnly>]
type private ResultAggregator<'T> (size : int) =
    let array = Array.zeroCreate<'T> size
    member __.Length = size
    member __.Values = array
    member __.Item with set i x = array.[i] <- x

/// Cloud process implementation that wraps around System.Threading.ProcessCompletionSource for inmemory runtimes
[<AutoSerializable(false); CloneableOnly>]
type private ThreadPoolProcessCompletionSource<'T> (?cancellationToken : ICloudCancellationToken) =
    let tcs = new TaskCompletionSource<'T>()
    let cts =
        match cancellationToken with
        | None -> ThreadPoolCancellationTokenSource()
        | Some ct -> ThreadPoolCancellationTokenSource.CreateLinkedCancellationTokenSource [|ct|]

    let task = new ThreadPoolProcess<'T>(tcs.Task, cts.Token)

    member __.CancellationTokenSource = cts
    member __.LocalProcessCompletionSource = tcs
    member __.Task = task

/// Collection of workflows that provide parallelism
/// using the .NET thread pool
type Combinators private () =

    static let cloneProtected (memoryEmulation : MemoryEmulation) (value : 'T) =
        try EmulatedValue.clone memoryEmulation value |> Choice1Of2
        with e -> Choice2Of2 e

    static let emulateProtected (memoryEmulation : MemoryEmulation) (value : 'T) =
        try EmulatedValue.create memoryEmulation true value |> Choice1Of2
        with e -> Choice2Of2 e

    /// <summary>
    ///     A Cloud.Parallel implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Parallel (mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, memoryEmulation : MemoryEmulation, computations : seq<#Cloud<'T>>) : LocalCloud<'T []> =
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // early detect if return type is not serializable.
            | Choice1Of2 _ when not <| MemoryEmulation.isShared memoryEmulation && not <| FsPickler.IsSerializableType<'T>() ->     
                let msg = sprintf "Cloud.Parallel workflow uses non-serializable type '%s'." Type.prettyPrint<'T>
                let e = new SerializationException(msg)
                cont.Exception ctx (ExceptionDispatchInfo.Capture e)

            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx [||]

            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] ->
                match cloneProtected memoryEmulation (comp, cont) with
                | Choice1Of2 (comp, cont) ->
                    let cont' = Continuation.map (fun t -> [| t |]) cont
                    Cloud.StartImmediateWithContinuations(comp, cont', ctx)

                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                match emulateProtected memoryEmulation (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 clonedComputations ->
                    let results = new ResultAggregator<'T>(computations.Length)
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let exceptionLatch = new ConcurrentCounter(0)
                    let completionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess (i : int) (cont : Continuation<'T[]>) ctx (t : 'T) =
                        match cloneProtected memoryEmulation t with
                        | Choice1Of2 t ->
                            results.[i] <- t
                            if completionLatch.Increment() = results.Length then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) results.Values

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." Type.prettyPrint<'T> 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onException (cont : Continuation<'T[]>) ctx edi =
                        match cloneProtected memoryEmulation edi with
                        | Choice1Of2 edi ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel ()
                                cont.Exception (revertCtx ctx) edi

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." Type.prettyPrint<'T> 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onCancellation (cont : Continuation<'T[]>) ctx c =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) c

                    for i = 0 to computations.Length - 1 do
                        // clone different continuation for each child
                        let computations,cont = clonedComputations.Value
                        let computation = computations.[i]
                        let ctx = { Resources = ctx.Resources ; CancellationToken = innerCts.Token }
                        let cont = { Success = onSuccess i cont ; Exception = onException cont ; Cancellation = onCancellation cont }
                        Cloud.StartWithContinuations(computation, cont, ctx))

    /// <summary>
    ///     A Cloud.Choice implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Choice(mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, memoryEmulation : MemoryEmulation, computations : seq<#Cloud<'T option>>) : LocalCloud<'T option> =
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx None
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] -> 
                match cloneProtected memoryEmulation (comp, cont) with
                | Choice1Of2 (comp, cont) -> Cloud.StartImmediateWithContinuations(comp, cont, ctx)
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                // distributed computation, ensure that closures are serializable
                match emulateProtected memoryEmulation (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 clonedComputations ->
                    let N = computations.Length // avoid capturing original computations in continuation closures
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let completionLatch = new ConcurrentCounter(0)
                    let exceptionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess (cont : Continuation<'T option>) ctx (topt : 'T option) =
                        if Option.isSome topt then
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) topt
                        else
                            if completionLatch.Increment () = N then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) None

                    let onException (cont : Continuation<'T option>) ctx edi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Exception (revertCtx ctx) edi

                    let onCancellation (cont : Continuation<'T option>) ctx cdi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) cdi

                    for i = 0 to computations.Length - 1 do
                        // clone different continuation for each child
                        let computations,cont = clonedComputations.Value
                        let computation = computations.[i]
                        let ctx = { Resources = ctx.Resources ; CancellationToken = innerCts.Token }
                        let cont = { Success = onSuccess cont ; Exception = onException cont ; Cancellation = onCancellation cont }
                        Cloud.StartWithContinuations(computation, cont, ctx))

    /// <summary>
    ///     A Cloud.StartAsTask implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    /// <param name="cancellationToken">Cancellation token for task. Defaults to new cancellation token.</param>
    static member StartAsTask (workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) =
        if memoryEmulation <> MemoryEmulation.Shared && not <| FsPickler.IsSerializableType<'T> () then
            let msg = sprintf "Cloud process returns non-serializable type '%s'." Type.prettyPrint<'T>
            raise <| new SerializationException(msg)


        match cloneProtected memoryEmulation workflow with
        | Choice2Of2 e ->
            let msg = sprintf "Cloud process of type '%s' uses non-serializable closure." Type.prettyPrint<'T>
            raise <| new SerializationException(msg, e)

        | Choice1Of2 workflow ->
            let clonedWorkflow = EmulatedValue.clone memoryEmulation workflow
            let tcs = new ThreadPoolProcessCompletionSource<'T>(?cancellationToken = cancellationToken)
            let setResult cont result =
                match cloneProtected memoryEmulation result with
                | Choice1Of2 result -> cont result |> ignore
                | Choice2Of2 e ->
                    let msg = sprintf "Could not serialize result for task of type '%s'." Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    ignore <| tcs.LocalProcessCompletionSource.TrySetException se

            let cont =
                {
                    Success = fun _ t -> t |> setResult tcs.LocalProcessCompletionSource.TrySetResult
                    Exception = fun _ (edi:ExceptionDispatchInfo) -> edi |> setResult (fun edi -> tcs.LocalProcessCompletionSource.TrySetException (edi.Reify(false, false)))
                    Cancellation = fun _ _ -> tcs.LocalProcessCompletionSource.TrySetCanceled() |> ignore
                }

            Cloud.StartWithContinuations(clonedWorkflow, cont, resources, tcs.CancellationTokenSource.Token)
            tcs.Task


    /// <summary>
    ///     A Cloud.ToAsync implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    static member ToAsync(workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry) : Async<'T> = async {
        let! ct = Async.CancellationToken
        let imct = new ThreadPoolCancellationToken(ct)
        let task = Combinators.StartAsTask(workflow, memoryEmulation, resources, cancellationToken = imct)
        return! Async.AwaitTaskCorrect task.LocalTask
    }

    /// <summary>
    ///     A Cloud.ToAsync implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    static member RunSynchronously(workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry, ?cancellationToken) : 'T =
        let task = Combinators.StartAsTask(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken)
        (task :> ICloudProcess<'T>).Result