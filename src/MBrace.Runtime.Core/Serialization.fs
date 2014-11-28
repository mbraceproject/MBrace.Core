module Nessos.MBrace.Runtime.Serialization

    open System.IO

    open Nessos.FsPickler
    open Nessos.MBrace.Store

    type FsPicklerStoreSerializer(fsp : FsPicklerSerializer) =
        let id = sprintf "FsPickler:%s" fsp.PickleFormat
        interface ISerializer with
            member __.Id = id
            member __.Serialize(stream, value : 'T) = fsp.Serialize(stream, value)
            member __.Deserialize<'T>(stream) = fsp.Deserialize<'T>(stream)
            member __.SeqSerialize(stream, values : 'T seq) = fsp.SerializeSequence(stream, values)
            member __.SeqDeserialize<'T>(stream, length : int) = fsp.DeserializeSequence<'T>(stream)

        static member Default = new FsPicklerStoreSerializer(VagrantRegistry.Pickler)

    /// Represents a typed serialized value.
    type Pickle<'T> private (data : byte []) =
        new (value : 'T) = new Pickle<'T>(VagrantRegistry.Pickler.Pickle(value))
        member __.UnPickle () = VagrantRegistry.Pickler.UnPickle<'T> data
        member __.Data = data

    /// Pickle methods
    [<RequireQualifiedAccess>]
    module Pickle =

        /// Pickles a value.
        let pickle value = new Pickle<'T>(value)
        /// Unpickles a value.
        let unpickle (p : Pickle<'T>) = p.UnPickle()