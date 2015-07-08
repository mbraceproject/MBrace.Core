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

/// Specifies memory semantics in parallel workflows
type ThreadPoolMemoryMode =
    /// All bindings shared among child workflows; Async semantics
    | Shared                = 0
    /// All bindings shared among child workflows; Async semantics.
    /// Emulate distributed serialization errors by checking that object graphs are serializable
    | EnsureSerializable    = 1
    /// Bindings copied for each child workflow; full distribution semantics.
    | Copy                  = 2

/// Collection of workflows that provide parallelism
/// using the .NET thread pool
type ThreadPool private () =

    static let scheduleTask res ct sc ec cc wf =
        Trampoline.QueueWorkItem(fun () ->
            let ctx = { Resources = res ; CancellationToken = ct }
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx))

    static let ensureSerializable (mode:ThreadPoolMemoryMode) (value : 'T) : Choice<'T, exn> =
        match mode with
        | ThreadPoolMemoryMode.Shared -> Choice1Of2 value
        | ThreadPoolMemoryMode.EnsureSerializable ->
            try FsPickler.EnsureSerializable value ; Choice1Of2 value
            with e -> Choice2Of2 e

        | ThreadPoolMemoryMode.Copy ->
            try FsPickler.Clone value |> Choice1Of2
            with e -> Choice2Of2 e

        | _ -> Choice1Of2 value

    /// <summary>
    ///     A Cloud.Parallel implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="computations">Input computations.</param>
    /// <param name="mode">Memory semantics used for parallelism. Defaults to shared memory.</param>
    static member Parallel (mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, computations : seq<#Cloud<'T>>, ?mode:ThreadPoolMemoryMode) : Local<'T []> =
        let mode = defaultArg mode ThreadPoolMemoryMode.Shared
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)

            // early detect if return type is not serializable.
            | Choice1Of2 _ when mode <> ThreadPoolMemoryMode.Shared && not <| FsPickler.IsSerializableType<'T>() ->     
                let msg = sprintf "Cloud.Parallel workflow uses non-serializable type '%s'." (Type.prettyPrint typeof<'T>)
                let e = new SerializationException(msg)
                cont.Exception ctx (ExceptionDispatchInfo.Capture e)

            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx [||]

            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] ->
                match ensureSerializable mode (comp, cont) with
                | Choice1Of2 (comp, cont) ->
                    let cont' = Continuation.map (fun t -> [| t |]) cont
                    Cloud.StartWithContinuations(comp, cont', ctx)

                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                match ensureSerializable mode (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 (computations, cont) ->
                    let results = Array.zeroCreate<'T> computations.Length
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let exceptionLatch = new ConcurrentCounter(0)
                    let completionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess i ctx (t : 'T) =
                        match ensureSerializable mode t with
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

                    let onException ctx edi =
                        match ensureSerializable mode edi with
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

                    let onCancellation ctx c =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) c

                    for i = 0 to computations.Length - 1 do
                        scheduleTask ctx.Resources innerCts.Token (onSuccess i) onException onCancellation computations.[i])

    /// <summary>
    ///     A Cloud.Choice implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="computations">Input computations.</param>
    /// <param name="mode">Memory semantics used for parallelism. Defaults to shared memory.</param>
    static member Choice(mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, computations : seq<#Cloud<'T option>>, ?mode : ThreadPoolMemoryMode) : Local<'T option> =
        let mode = defaultArg mode ThreadPoolMemoryMode.Shared
        Local.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx None
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] -> 
                match ensureSerializable mode (comp, cont) with
                | Choice1Of2 (comp, cont) -> Cloud.StartWithContinuations(comp, cont, ctx)
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                // distributed computation, ensure that closures are serializable
                match ensureSerializable mode (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 (computations, cont) ->
                    let N = computations.Length
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let completionLatch = new ConcurrentCounter(0)
                    let exceptionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess ctx (topt : 'T option) =
                        if Option.isSome topt then
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) topt
                        else
                            if completionLatch.Increment () = N then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) None

                    let onException ctx edi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Exception (revertCtx ctx) edi

                    let onCancellation ctx cdi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) cdi

                    for i = 0 to computations.Length - 1 do
                        scheduleTask ctx.Resources innerCts.Token onSuccess onException onCancellation computations.[i])