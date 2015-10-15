namespace MBrace.Core.Internals

open System
open System.Reflection

#nowarn "444"

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
        let newTrace =
            if useSeparator && not <| String.IsNullOrEmpty sourceStackTrace then
                sourceStackTrace + Environment.NewLine + separator
            else
                sourceStackTrace

        try ()
        finally
            lock sourceExn (fun () ->
                if prepareForRaise then
                    let newStackTrace = if String.IsNullOrEmpty newTrace then newTrace else newTrace + Environment.NewLine
                    trySetRemoteStackTrace newStackTrace sourceExn |> ignore
                else
                    trySetStackTrace newTrace sourceExn |> ignore
                    trySetRemoteStackTrace null sourceExn |> ignore)

        sourceExn

    /// <summary>
    ///     Creates a new ExceptionDispatchInfo instance with line appended to stacktrace.
    /// </summary>
    /// <param name="line">Line to be appended.</param>
    member __.AppendToStackTrace(line : string) = 
        let newTrace =
            if String.IsNullOrWhiteSpace sourceStackTrace then line
            else
                sourceStackTrace + Environment.NewLine + line

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