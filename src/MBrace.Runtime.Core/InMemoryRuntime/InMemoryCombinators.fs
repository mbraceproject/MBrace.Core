namespace MBrace.Runtime.InMemoryRuntime

#nowarn "444"

open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

open Nessos.FsPickler

/// Collection of workflows that provide parallelism
/// using the .NET thread pool
type ThreadPool private () =

    static let scheduleTask res ct sc ec cc wf =
        Trampoline.QueueWorkItem(fun () ->
            let ctx = { Resources = res ; CancellationToken = ct }
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx))

    static let cloneProtected (mode : MemoryEmulation) (value : 'T) =
        try EmulatedValue.clone mode value |> Choice1Of2
        with e -> Choice2Of2 e

    static let emulateProtected (mode : MemoryEmulation) (value : 'T) =
        try EmulatedValue.create mode true value |> Choice1Of2
        with e -> Choice2Of2 e

    /// <summary>
    ///     A Cloud.Parallel implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="mode">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Parallel (mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, mode : MemoryEmulation, computations : seq<#Cloud<'T>>) : Local<'T []> =
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // early detect if return type is not serializable.
            | Choice1Of2 _ when not <| MemoryEmulation.isShared mode && not <| FsPickler.IsSerializableType<'T>() ->     
                let msg = sprintf "Cloud.Parallel workflow uses non-serializable type '%s'." (Type.prettyPrint typeof<'T>)
                let e = new SerializationException(msg)
                cont.Exception ctx (ExceptionDispatchInfo.Capture e)

            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx [||]

            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] ->
                match cloneProtected mode (comp, cont) with
                | Choice1Of2 (comp, cont) ->
                    let cont' = Continuation.map (fun t -> [| t |]) cont
                    Cloud.StartWithContinuations(comp, cont', ctx)

                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                match emulateProtected mode (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 clonedComputations ->
                    let results = Array.zeroCreate<'T> computations.Length
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let exceptionLatch = new ConcurrentCounter(0)
                    let completionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess i (cont : Continuation<'T[]>) ctx (t : 'T) =
                        match cloneProtected mode t with
                        | Choice1Of2 t ->
                            results.[i] <- t
                            if completionLatch.Increment() = results.Length then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) results

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." (Type.prettyPrint typeof<'T>) 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onException (cont : Continuation<'T[]>) ctx edi =
                        match cloneProtected mode edi with
                        | Choice1Of2 edi ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel ()
                                cont.Exception (revertCtx ctx) edi

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." (Type.prettyPrint typeof<'T>) 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onCancellation (cont : Continuation<'T[]>) ctx c =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) c

                    for i = 0 to computations.Length - 1 do
                        // clone different continuation for each child
                        let computations,cont = clonedComputations.Value
                        scheduleTask ctx.Resources innerCts.Token (onSuccess i cont) (onException cont) (onCancellation cont) computations.[i])

    /// <summary>
    ///     A Cloud.Choice implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="mode">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Choice(mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, mode : MemoryEmulation, computations : seq<#Cloud<'T option>>) : Local<'T option> =
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx None
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] -> 
                match cloneProtected mode (comp, cont) with
                | Choice1Of2 (comp, cont) -> Cloud.StartWithContinuations(comp, cont, ctx)
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                // distributed computation, ensure that closures are serializable
                match emulateProtected mode (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
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
                        scheduleTask ctx.Resources innerCts.Token (onSuccess cont) (onException cont) (onCancellation cont) computations.[i])

    /// <summary>
    ///     A Cloud.StartAsTask implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="mode">Memory semantics used for parallelism.</param>
    /// <param name="faultPolicy">Fault policy for computation. Defaults to current fault policy.</param>
    /// <param name="cancellationToken">Cancellation token for task. Defaults to new cancellation token.</param>
    static member StartAsTask (workflow:Cloud<'T>, mode:MemoryEmulation, ?faultPolicy:FaultPolicy, ?cancellationToken:ICloudCancellationToken) = cloud {
        let! resources = Cloud.GetResourceRegistry()
        let dp = resources.Resolve<IDistributionProvider> ()
        let dp' = match faultPolicy with None -> dp | Some fp -> dp.WithFaultPolicy fp
        let resources' = resources.Register dp'
        let cancellationToken = match cancellationToken with Some ct -> ct | None -> new InMemoryCancellationToken() :> _
        let clonedWorkflow = EmulatedValue.clone mode workflow
        let task = Cloud.StartAsSystemTask(clonedWorkflow, resources', cancellationToken)
        return new InMemoryTask<'T>(task, cancellationToken) :> ICloudTask<'T>
    }