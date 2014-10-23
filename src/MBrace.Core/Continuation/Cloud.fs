namespace Nessos.MBrace

open System
open System.Threading

open Nessos.MBrace.Runtime

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
    ///   Initializes an empty execution context.  
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    static member Empty(?cancellationToken : CancellationToken) =
        {
            Resources = ResourceRegistry.Empty
            CancellationToken = match cancellationToken with Some ct -> ct | None -> new CancellationToken()
        }

/// Execution context for continuation callbacks
type Continuation<'T> =
    {
        /// Success continuation
        Success : ExecutionContext -> 'T -> unit

        /// Exception continuation
        Exception : ExecutionContext -> exn -> unit

        /// Cancellation continuation
        Cancellation : ExecutionContext -> OperationCanceledException -> unit
    }

/// Cloud workflow of type T
and Cloud<'T> = internal Body of (ExecutionContext -> Continuation<'T> -> unit)