namespace Nessos.MBrace

open System
open System.Collections
open System.Collections.Generic
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

/// Represents a finite and immutable sequence of
/// elements that is stored in the underlying CloudStore
/// and will be enumerated on demand.
[<Sealed; AutoSerializable(true)>]
type CloudSeq<'T> private (path : string, length : int, fileStore : ICloudFileStore, serializer : ISerializer) =

    let serializerId = serializer.GetSerializerDescriptor()
    let storeId = fileStore.GetFileStoreDescriptor()

    [<NonSerialized>]
    let mutable serializer = Some serializer
    [<NonSerialized>]
    let mutable fileStore = Some fileStore

    let getFileStore() = 
        match fileStore with
        | Some s -> s
        | None ->
            let s = storeId.Recover()
            fileStore <- Some s
            s

    let getSerializer() = 
        match serializer with
        | Some s -> s
        | None ->
            let s = serializerId.Recover()
            serializer <- Some s
            s
    
    let getSequenceAsync() = async {
        let! stream = getFileStore().BeginRead(path)
        return getSerializer().SeqDeserialize<'T>(stream, length)
    }

    let getSequence () = getSequenceAsync () |> Async.RunSync

    /// Path to CloudSeq in store
    member __.Path = path
    /// Sequence length
    member __.Length = length
    /// Asynchronously fetches the sequence
    member __.GetSequenceAsync() = getSequenceAsync()

    interface ICloudDisposable with
        member __.Dispose() = async { do! getFileStore().DeleteFile path }

    interface IEnumerable<'T> with
        member __.GetEnumerator() = (getSequence() :> IEnumerable).GetEnumerator()
        member __.GetEnumerator() = getSequence().GetEnumerator()

    static member CreateAsync (values : seq<'T>, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let fileName = fileStore.GetRandomFilePath directory
        let! length = async { use! stream = fileStore.BeginWrite fileName in return serializer.SeqSerialize(stream, values) }
        return new CloudSeq<'T>(fileName, length, fileStore, serializer)
    }


#nowarn "444"

type CloudSeq =

    /// <summary>
    ///     Creates a new cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Collection to populate the cloud sequence with.</param>
    /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member New(values : seq<'T>, ?directory, ?serializer) : Cloud<CloudSeq<'T>> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let path = defaultArg directory config.DefaultDirectory
        return! Cloud.OfAsync <| CloudSeq<'T>.CreateAsync(values, path, config.FileStore, serializer)
    }