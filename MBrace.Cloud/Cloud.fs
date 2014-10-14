namespace Nessos.MBrace

    open System
    open System.Threading

    type IResourceResolver =
        abstract Resolve<'TResource> : unit -> 'TResource

    [<AutoSerializable(false)>]
    type Context<'T> =
        {
            Resource : IResourceResolver

            CancellationToken : CancellationToken

            scont : 'T -> unit
            econt : exn -> unit
            ccont : OperationCanceledException -> unit
        }

    and Cloud<'T> = internal Body of (Context<'T> -> unit)