namespace Nessos.MBrace.Runtime

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
        Exception : ExecutionContext -> exn -> unit

        /// Cancellation continuation
        Cancellation : ExecutionContext -> OperationCanceledException -> unit
    }