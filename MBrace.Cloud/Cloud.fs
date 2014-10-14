namespace Nessos.MBrace

    open System

    type IResourceResolver =
        abstract Resolve<'Eff> : unit -> 'Eff

    and Context<'T> =
        {
            Resource : IResourceResolver

            scont : 'T -> unit
            econt : exn -> unit
            ccont : OperationCanceledException -> unit
        }

    and Cloud<'T> = internal Body of (Context<'T> -> unit)