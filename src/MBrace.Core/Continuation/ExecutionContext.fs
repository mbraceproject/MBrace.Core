namespace MBrace.Continuation

open System
open System.Threading

/// Local, non-distributable continuation execution context.
[<AutoSerializable(false)>]
type ExecutionContext =
    {
        /// Runtime cloud resource resolver
        Resources : ResourceRegistry

        /// Local cancellation token
        CancellationToken : CancellationToken
    }
with
    /// <summary>
    ///     Initializes an empty execution context.  
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    static member Empty(?cancellationToken : CancellationToken) =
        {
            Resources = ResourceRegistry.Empty
            CancellationToken = match cancellationToken with Some ct -> ct | None -> new CancellationToken()
        }

/// Distributable continuation context.
[<AutoSerializable(true)>]
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
            Success = fun ctx s -> tcont.Success ctx (f s)
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