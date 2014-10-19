namespace Nessos.MBrace

open System

open Nessos.MBrace.Runtime

/// Execution context for continuation callbacks
[<AutoSerializable(false)>]
type Context<'T> =
    {
        /// Runtime cloud resource resolver
        Resource : ResourceRegistry

        /// Local cancellation token
        CancellationToken : System.Threading.CancellationToken

        /// Success continuation
        scont : 'T -> unit

        /// Exception continuation
        econt : exn -> unit

        /// Cancellation continuation
        ccont : OperationCanceledException -> unit
    }

/// Cloud workflow of type T
and Cloud<'T> = internal Body of (Context<'T> -> unit)