namespace Nessos.MBrace

    /// Cloud resource runtime dependency resolver
    type IResourceResolver =
        /// Resolves resource of given type
        abstract Resolve<'TResource> : unit -> 'TResource

    /// Execution context for continuation callbacks
    [<AutoSerializable(false)>]
    type Context<'T> =
        {
            /// Runtime cloud resource resolver
            Resource : IResourceResolver

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
    type Cloud<'T> = internal Body of (Context<'T> -> unit)