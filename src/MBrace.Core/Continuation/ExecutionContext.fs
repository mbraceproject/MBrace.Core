namespace MBrace.Core.Internals

open System
open System.Threading

open MBrace.Core

/// Local, non-distributable continuation execution context.
[<AutoSerializable(false) ; NoEquality; NoComparison>]
type ExecutionContext =
    {
        /// Cloud cancellation token of the current context
        CancellationToken : ICloudCancellationToken

        /// Runtime cloud resource resolver
        Resources : ResourceRegistry
    }
with
    /// <summary>
    ///     Initializes an empty execution context.  
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    static member Empty(cancellationToken : ICloudCancellationToken) =
        {
            Resources = ResourceRegistry.Empty
            CancellationToken = cancellationToken
        }

/// Distributable continuation context.
[<AutoSerializable(true) ; NoEquality; NoComparison>]
type Continuation<'T> =
    {
        /// Success continuation
        Success : ExecutionContext -> 'T -> unit

        /// Exception continuation
        Exception : ExecutionContext -> ExceptionDispatchInfo -> unit

        /// Cancellation continuation
        Cancellation : ExecutionContext -> OperationCanceledException -> unit
    }

/// Continuation utility functions
[<RequireQualifiedAccess>]
module Continuation =
    
    /// <summary>
    ///     Contravariant Continuation map combinator.
    /// </summary>
    /// <param name="f">Mapper function.</param>
    /// <param name="tcont">Initial continuation.</param>
    let inline map (f : 'S -> 'T) (tcont : Continuation<'T>) : Continuation<'S> =
        {
            Success = 
                fun ctx s -> 
                    let r = ValueOrException.protect f s
                    if r.IsValue then 
                        tcont.Success ctx r.Value
                    else
                        tcont.Exception ctx (ExceptionDispatchInfo.Capture r.Exception)

            Exception = tcont.Exception
            Cancellation = tcont.Cancellation
        }

    /// <summary>
    ///     Contravariant failure combinator
    /// </summary>
    /// <param name="f">Mapper function.</param>
    /// <param name="tcont">Initial continuation.</param>
    let inline failwith (f : 'S -> exn) (tcont : Continuation<'T>) : Continuation<'S> =
        {
            Success = fun ctx s -> tcont.Exception ctx (ExceptionDispatchInfo.Capture (try f s with e -> e))
            Exception = tcont.Exception
            Cancellation = tcont.Cancellation
        }

    /// <summary>
    ///     Contravariant Continuation choice combinator.
    /// </summary>
    /// <param name="f">Choice function.</param>
    /// <param name="tcont">Initial continuation.</param>
    let inline choice (f : 'S -> Choice<'T, exn>) (tcont : Continuation<'T>) : Continuation<'S> =
        {
            Success = fun ctx s -> 
                match (try f s with e -> Choice2Of2 e) with 
                | Choice1Of2 t -> tcont.Success ctx t 
                | Choice2Of2 e -> tcont.Exception ctx (ExceptionDispatchInfo.Capture e)

            Exception = tcont.Exception
            Cancellation = tcont.Cancellation
        }