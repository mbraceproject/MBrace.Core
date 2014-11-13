namespace Nessos.MBrace.Runtime

open System
open System.Reflection
open System.Threading.Tasks

/// Replacement for System.Runtime.ExceptionServices.ExceptionDispatchInfo
/// that is serializable and permits symbolic appending to stacktrace
[<Sealed; AutoSerializable(true)>]
type ExceptionDispatchInfo private (sourceExn : exn, sourceStackTrace : string) =

    [<Literal>]
    static let separator = "--- End of stack trace from previous location where exception was thrown ---"

    // resolve the internal stacktrace field in exception
    // this is implementation-sensitive so not guaranteed to work. 
    static let bindingFlags = BindingFlags.NonPublic ||| BindingFlags.Instance
    static let stackTraceField : FieldInfo =
        match typeof<System.Exception>.GetField("_stackTraceString", bindingFlags) with
        | null -> typeof<System.Exception>.GetField("stack_trace", bindingFlags)
        | f -> f

    static let remoteStackTraceField : FieldInfo =
        match typeof<System.Exception>.GetField("remote_stack_trace", bindingFlags) with
        | null -> typeof<System.Exception>.GetField("_remoteStackTraceString", bindingFlags)
        | f -> f

    static let trySetStackTrace (trace : string) (e : exn) =
        match stackTraceField with
        | null -> false
        | f -> f.SetValue(e, trace) ; true

    static let trySetRemoteStackTrace (trace : string) (e : exn) =
        match remoteStackTraceField with
        | null -> false
        | f -> f.SetValue(e, trace) ; true

    /// <summary>
    ///     Captures the provided exception stacktrace into an ExceptionDispatchInfo instance.
    /// </summary>
    /// <param name="exn">Captured exception</param>
    static member Capture(exn : exn) =
        if exn = null then invalidArg "exn" "argument cannot be null."
        new ExceptionDispatchInfo(exn, exn.StackTrace)

    /// <summary>
    ///     Returns contained exception with restored stacktrace state.
    ///     This operation mutates exception contents, so should be used with care.
    /// </summary>
    /// <param name="useSeparator">Add a separator after remote stacktrace. Defaults to true.</param>
    /// <param name="prepareForRaise">Prepare exception state for raise. Defaults to false.</param>
    member __.Reify (?useSeparator : bool, ?prepareForRaise : bool) : exn =
        let useSeparator = defaultArg useSeparator true
        let prepareForRaise = defaultArg prepareForRaise false
        lock sourceExn (fun () -> 
            try ()
            finally
                let newTrace =
                    if useSeparator then
                        sourceStackTrace + Environment.NewLine + separator
                    else
                        sourceStackTrace

                if prepareForRaise then
                    trySetRemoteStackTrace (newTrace + Environment.NewLine) sourceExn |> ignore
                else
                    trySetStackTrace newTrace sourceExn |> ignore
                    trySetRemoteStackTrace null sourceExn |> ignore

            sourceExn)

    /// <summary>
    ///     Creates a new ExceptionDispatchInfo instance with line appended to stacktrace.
    /// </summary>
    /// <param name="line">Line to be appended.</param>
    member __.AppendToStackTrace(line : string) = 
        let newTrace = sprintf "%s%s%s" sourceStackTrace Environment.NewLine line
        new ExceptionDispatchInfo(sourceExn, newTrace)

    /// <summary>
    ///     Creates a new ExceptionDispatchInfo instance with line appended to stacktrace.
    /// </summary>
    /// <param name="line">Line to be appended.</param>
    member __.AppendToStackTrace(lines : seq<string>) = 
        __.AppendToStackTrace(String.concat Environment.NewLine lines)

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ExceptionDispatchInfo =

    /// <summary>
    ///     Raise provided exception dispatch info.
    /// </summary>
    /// <param name="useSeparator">Appends a stacktrace separator after the remote stacktrace.</param>
    /// <param name="edi">Exception dispatch info to be raised.</param>
    let inline raise useSeparator (edi : ExceptionDispatchInfo) =
        raise <| edi.Reify(useSeparator, prepareForRaise = true)

    /// <summary>
    ///     Immediately raises exception instance, preserving its current stacktrace
    /// </summary>
    /// <param name="useSeparator">Appends a stacktrace separator after the remote stacktrace.</param>
    /// <param name="exn">Input exception.</param>
    let inline raiseWithCurrentStackTrace useSeparator (exn : 'exn) =
        let edi = ExceptionDispatchInfo.Capture exn in raise useSeparator edi

[<AutoOpen>]
module ExceptionDispatchInfoUtils =

    type Async =

        /// <summary>
        ///     Efficiently reraise exception, without losing its existing stacktrace.
        /// </summary>
        /// <param name="e"></param>
        static member Raise<'T> (e : exn) : Async<'T> = Async.FromContinuations(fun (_,ec,_) -> ec e)

        /// <summary>
        ///     Runs the asynchronous computation and awaits its result.
        ///     Preserves original stacktrace for any exception raised.
        /// </summary>
        /// <param name="workflow">Workflow to be run.</param>
        /// <param name="cancellationToken">Optioncal cancellation token.</param>
        static member RunSync(workflow : Async<'T>, ?cancellationToken) =
            let tcs = new TaskCompletionSource<Choice<'T,exn,OperationCanceledException>>()
            let inline commit f r = tcs.SetResult(f r)
            Trampoline.QueueWorkItem(fun () ->
                Async.StartWithContinuations(workflow, 
                    commit Choice1Of3, commit Choice2Of3, commit Choice3Of3, 
                    ?cancellationToken = cancellationToken))

            match tcs.Task.Result with
            | Choice1Of3 t -> t
            | Choice2Of3 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e
            | Choice3Of3 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e