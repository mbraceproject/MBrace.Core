namespace Nessos.MBrace

    /// Execution context for continuation callbacks
    [<AutoSerializable(false)>]
    type Context<'T> =
        {
            /// Runtime cloud resource resolver
            Resource : ResourceResolver

            /// Local cancellation token
            CancellationToken : System.Threading.CancellationToken

            /// Success continuation
            scont : 'T -> unit

            /// Exception continuation
            econt : exn -> unit

            /// Cancellation continuation
            ccont : System.OperationCanceledException -> unit
        }

    /// Cloud workflow of type T
    and Cloud<'T> = internal Body of (Context<'T> -> unit)