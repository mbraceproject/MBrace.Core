module Nessos.MBrace.Runtime.Serialization

open System.IO
open System.Collections.Concurrent

open Nessos.FsPickler
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime.Utils

type FsPicklerStoreSerializer private (fsp : FsPicklerSerializer, id : string) =
    static let picklerRegistry = new ConcurrentDictionary<string, FsPicklerStoreSerializer> ()

    static let resolve (id : string) =
        let mutable v = Unchecked.defaultof<_>
        let ok = picklerRegistry.TryGetValue(id, &v)
        if ok then v
        else
            invalidOp "FsPicklerStoreSerializer: could not resolve serializer of id '%O'." id

    static member Create(fsp : FsPicklerSerializer, ?id : string) =
        let id = match id with Some i -> i | None -> sprintf "FsPickler:%s" fsp.PickleFormat
        new FsPicklerStoreSerializer(fsp, id)

    static member CreateAndRegister(fsp : FsPicklerSerializer, ?id : string, ?forceUpdate, ?throwOnError) =
        let fspss = FsPicklerStoreSerializer.Create(fsp, ?id = id)
        let isSuccess = picklerRegistry.TryAdd(fspss.Id, fspss, ?forceUpdate = forceUpdate)
        if defaultArg throwOnError true && not isSuccess then
            invalidOp "FsPicklerStoreSerializer: a serializer of id '%O' already exists." id
        
        fspss

    static member Resolve(id : string) = resolve id

    member __.Id = id

    interface ISerializer with
        member __.Id = id
        member __.GetSerializerDescriptor() =
            let id = id
            {
                new ISerializerDescriptor with
                    member __.Id = id
                    member __.Recover () = FsPicklerStoreSerializer.Resolve id :> _
            }

        member __.Serialize(stream, value : 'T, leaveOpen) = fsp.Serialize(stream, value, leaveOpen = leaveOpen)
        member __.Deserialize<'T>(stream, leaveOpen) = fsp.Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member __.SeqSerialize(stream, values : 'T seq, leaveOpen) = fsp.SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member __.SeqDeserialize<'T>(stream, length : int, leaveOpen) = fsp.DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)