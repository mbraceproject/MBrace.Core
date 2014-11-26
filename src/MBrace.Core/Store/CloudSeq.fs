namespace Nessos.MBrace

open System
open System.Collections
open System.Collections.Generic
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

[<Sealed; AutoSerializable(true)>]
type CloudSeq<'T> private (path : string, length : int, store : ICloudStore, serializer : ISerializer) =

    let storeId = store.UUID
    let serializerId = serializer.Id

    [<NonSerialized>]
    let mutable store = Some store.FileStore
    [<NonSerialized>]
    let mutable serializer = Some serializer

    let getStore() =
        match store with
        | Some s -> s
        | None ->
            let s = CloudStoreRegistry.Resolve(storeId).FileStore
            store <- Some s
            s
    
    let getSequenceAsync() = async {
        let s = match serializer with Some s -> s | None -> SerializerRegistry.Resolve serializerId
        let! stream = getStore().BeginRead path
        return s.SeqDeserialize<'T>(stream, length)
    }

    let getSequence () = getSequenceAsync () |> Async.RunSync

    member __.Id = path
    member __.Length = length
    member __.GetSequenceAsync() = getSequenceAsync()

    interface ICloudDisposable with
        member __.Dispose() = getStore().DeleteFile(path)

    interface IEnumerable<'T> with
        member __.GetEnumerator() = (getSequence() :> IEnumerable).GetEnumerator()
        member __.GetEnumerator() = getSequence().GetEnumerator()

    static member internal Create (values : seq<'T>, container : string, store : ICloudStore, serializer : ISerializer) = async {
        let fileName = store.FileStore.CreateUniqueFileName container
        use! stream = store.FileStore.BeginWrite(fileName)
        let length = serializer.SeqSerialize(stream, values)
        return new CloudSeq<'T>(fileName, length, store, serializer)
    }


namespace Nessos.MBrace.Store

open Nessos.MBrace

[<AutoOpen>]
module CloudSeqUtils =

    type CloudStoreConfiguration with
        /// <summary>
        ///     Creates a new CloudSeq instance
        /// </summary>
        /// <param name="values">Values to be serialized.</param>
        /// <param name="container">FileStore container used for cloud ref. Defaults to configuration container.</param>
        /// <param name="serializer">Serialization used for object serialization. Default to configuration serializer.</param>
        member csc.CreateCloudSeq<'T>(values : seq<'T>, ?container : string, ?serializer) =
            let container = defaultArg container csc.DefaultContainer
            let serializer = defaultArg serializer csc.Serializer
            CloudSeq<'T>.Create(values, container, csc.Store, serializer)