namespace Nessos.MBrace.Runtime

open System
open System.Reflection
#if NET40
#else
open System.Runtime.ExceptionServices
#endif

[<AutoOpen>]
module Utils =

    // resolve the internal stacktrace field in exception
    // this is implementation-sensitive so not guarantee to work. 
    let private remoteStackTraceField : FieldInfo =
        let bfs = BindingFlags.NonPublic ||| BindingFlags.Instance
        match typeof<System.Exception>.GetField("remote_stack_trace", bfs) with
        | null ->
            match typeof<System.Exception>.GetField("_remoteStackTraceString", bfs) with
            | null -> null
            | f -> f
        | f -> f

    type Exception with
        /// <summary>
        ///     Set a custom stacktrace to given exception.
        /// </summary>
        /// <param name="trace">Stacktrace to be set.</param>
        member e.SetStackTrace(trace : string) =
            if obj.ReferenceEquals(remoteStackTraceField, null) then
                failwith "Could not locate RemoteStackTrace field for System.Exception."
            
            remoteStackTraceField.SetValue(e, trace)

    /// <summary>
    ///     Raise exception with given stacktrace
    /// </summary>
    /// <param name="trace">Stacktrace to be set.</param>
    /// <param name="e">exception to be raised.</param>
    let inline raiseWithStackTrace (trace : string) (e : #exn) =
        do e.SetStackTrace trace
        raise e

    let private reraiseMessage = 
        let nl = System.Environment.NewLine in
        nl + "--- End of stack trace from previous location where exception was thrown ---" + nl

    /// <summary>
    ///     Raises an exception while preserving its currently accummulated stacktrace.
    /// </summary>
    /// <param name="e">exception to be reraised.</param>
    let inline raiseWithCurrentStacktrace (e : #exn) : 'T = 
#if NET40
        raiseWithStackTrace (e.StackTrace + reraiseMessage) e
#else
        let edi = ExceptionDispatchInfo.Capture e
        edi.Throw()
        Unchecked.defaultof<'T> // force a generic type, won't be called
#endif

    /// returns a unique type identifier
    let inline internal typeId<'T> = typeof<'T>.AssemblyQualifiedName