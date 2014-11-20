namespace Nessos.MBrace.Store

open System
open System.IO
open System.Runtime.Serialization

open Nessos.MBrace

[<Sealed; AutoSerializable(true)>]
type CloudAtom<'T> =

    [<NonSerialized>]
    val mutable private provider : ICloudAtomProvider
    val private providerId : string
    val private id : string

    internal new (provider : ICloudAtomProvider, id : string) =
        {
            provider = provider
            providerId = ResourceRegistry<ICloudAtomProvider>.GetId provider
            id = id
        }

    [<OnDeserializedAttribute>]
    member private __.OnDeserialized(_ : StreamingContext) =
        __.provider <- ResourceRegistry<ICloudAtomProvider>.Resolve __.providerId

    member __.Id = id

    member __.Update(updater : 'T -> 'T) = __.provider.Update(__.id, updater)
    member __.Force(value : 'T) = __.provider.Force(__.id, value)

    interface ICloudDisposable with
        member __.Dispose () = __.provider.Delete __.id
    
and ICloudAtomProvider =
    inherit IResource

    abstract Exists : id:string -> Async<bool>
    abstract Delete : id:string -> Async<unit>

    abstract Create<'T> : initial:'T -> Async<string>
    abstract Update : id:string * updater:('T -> 'T) -> Async<unit> // retry policy?
    abstract Force : id:string * value:'T -> Async<unit>

    abstract Enumerate : unit -> Async<string []>