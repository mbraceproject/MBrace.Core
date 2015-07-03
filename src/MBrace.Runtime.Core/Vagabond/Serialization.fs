namespace MBrace.Runtime.Vagabond

open System
open System.IO
open System.Collections.Concurrent
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Vagabond

[<AbstractClass; AutoSerializable(true)>]
type FsPicklerStoreSerializer () as self =
    // force exception in case of Vagrant instance not initialized
    static do VagabondRegistry.Instance |> ignore

    // serializer instance registry for local AppDomain
    static let localInstances = new ConcurrentDictionary<string, FsPicklerSerializer> ()

    [<NonSerialized>]
    let mutable localInstance : FsPicklerSerializer option = None

    let getLocalInstance () =
        match localInstance with
        | Some instance -> instance
        | None ->
            // local instance not assigned, look up from registry
            let instance = localInstances.GetOrAdd(self.Id, ignore >> self.CreateLocalSerializerInstance)
            localInstance <- Some instance
            instance

    member __.Pickler = getLocalInstance()

    abstract Id : string
    abstract CreateLocalSerializerInstance : unit -> FsPicklerSerializer

    interface ISerializer with
        member __.Id = __.Id
        member __.Serialize (target : Stream, value : 'T, leaveOpen : bool) = getLocalInstance().Serialize(target, value, leaveOpen = leaveOpen)
        member __.Deserialize<'T>(stream, leaveOpen) = getLocalInstance().Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member __.SeqSerialize(stream, values : 'T seq, leaveOpen) = getLocalInstance().SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member __.SeqDeserialize<'T>(stream, leaveOpen) = getLocalInstance().DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)

[<AutoSerializable(true)>]
type FsPicklerBinaryStoreSerializer () =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler binary serializer"
    override __.CreateLocalSerializerInstance () = VagabondRegistry.Instance.Serializer :> _


[<AutoSerializable(true)>]
type FsPicklerXmlStoreSerializer (?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler xml serializer"
    override __.CreateLocalSerializerInstance () = FsPickler.CreateXml(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent) :> _

[<AutoSerializable(true)>]
type FsPicklerJsonStoreSerializer (?omitHeader : bool, ?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler json serializer"
    override __.CreateLocalSerializerInstance () = FsPickler.CreateJson(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent, ?omitHeader = omitHeader) :> _