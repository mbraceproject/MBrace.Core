namespace MBrace.Runtime

open System
open System.IO
open System.Runtime.Serialization
open System.Collections.Concurrent

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Core.Internals

/// Abstract FsPickler ISerializer implementation
[<AbstractClass; AutoSerializable(true)>]
type FsPicklerStoreSerializer () =

    [<NonSerialized>]
    let mutable localInstance : FsPicklerSerializer option = None

    /// Serializer identifier
    abstract Id : string
    /// Creates a new FsPickler serializer instance corresponding to the implementation.
    abstract CreateSerializer : unit -> FsPicklerSerializer

    member s.Serializer =
        match localInstance with
        | Some instance -> instance
        | None ->
            let instance = s.CreateSerializer()
            localInstance <- Some instance
            instance

    interface ISerializer with
        member s.Id = s.Id
        member s.IsSerializable(value : 'T) = try FsPickler.EnsureSerializable value ; true with _ -> false
        member s.Serialize (target : Stream, value : 'T, leaveOpen : bool) = s.Serializer.Serialize(target, value, leaveOpen = leaveOpen)
        member s.Deserialize<'T>(stream, leaveOpen) = s.Serializer.Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member s.SeqSerialize(stream, values : 'T seq, leaveOpen) = s.Serializer.SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member s.SeqDeserialize<'T>(stream, leaveOpen) = s.Serializer.DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)
        member s.ComputeObjectSize<'T>(graph:'T) = s.Serializer.ComputeSize graph
        member s.Clone(graph:'T) = FsPickler.Clone graph

/// Serializable binary FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type VagabondFsPicklerBinarySerializer () =
    inherit FsPicklerStoreSerializer()
    do ignore VagabondRegistry.Instance

    override __.Id = "Vagabond FsPickler binary serializer"
    override __.CreateSerializer () = 
        VagabondRegistry.Instance.Serializer :> _


/// Serializable xml FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type VagabondFsPicklerXmlSerializer (?indent : bool) =
    inherit FsPicklerStoreSerializer()
    do ignore VagabondRegistry.Instance

    override __.Id = "Vagabond FsPickler xml serializer"
    override __.CreateSerializer () = 
        FsPickler.CreateXmlSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent) :> _

/// Serializable json FsPickler implementation of ISerializer that targets
/// the underlying Vagabond registry
[<AutoSerializable(true)>]
type VagabondFsPicklerJsonSerializer (?omitHeader : bool, ?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "Vagabond FsPickler json serializer"
    override __.CreateSerializer () = 
        FsPickler.CreateJsonSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent, ?omitHeader = omitHeader) :> _