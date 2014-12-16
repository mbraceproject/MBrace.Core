module Nessos.MBrace.Runtime.Serialization

open System.IO
open System.Linq
open System.Collections.Concurrent

open Nessos.FsPickler
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime.Utils

[<AbstractClass>]
type FsPicklerStoreSerializer () =
    abstract Id : string
    abstract Serializer : FsPicklerSerializer

    interface ISerializer with
        member s.Id = s.Id
        member s.Serialize(stream, value : 'T, leaveOpen) = s.Serializer.Serialize(stream, value, leaveOpen = leaveOpen)
        member s.Deserialize<'T>(stream, leaveOpen) = s.Serializer.Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member s.SeqSerialize(stream, values : 'T seq, leaveOpen) = s.Serializer.SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member s.SeqDeserialize<'T>(stream, leaveOpen) = s.Serializer.DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)