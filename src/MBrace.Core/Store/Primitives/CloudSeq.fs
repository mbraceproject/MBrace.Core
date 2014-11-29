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
type CloudSeq<'T> private (path : string, length : int, file : CloudFile, serializer : ISerializer) =

    let serializerId = serializer.Id

    [<NonSerialized>]
    let mutable serializer = Some serializer
    
    let getSequenceAsync() = async {
        let s = match serializer with Some s -> s | None -> StoreRegistry.GetSerializer serializerId
        let! stream = file.BeginRead()
        return s.SeqDeserialize<'T>(stream, length)
    }

    let getSequence () = getSequenceAsync () |> Async.RunSync

    /// Path to CloudSeq in store
    member __.Path = path
    /// Sequence length
    member __.Length = length
    /// Asynchronously fetches the sequence
    member __.GetSequenceAsync() = getSequenceAsync()

    interface ICloudDisposable with
        member __.Dispose() = (file :> ICloudDisposable).Dispose()

    interface IEnumerable<'T> with
        member __.GetEnumerator() = (getSequence() :> IEnumerable).GetEnumerator()
        member __.GetEnumerator() = getSequence().GetEnumerator()

    static member internal Create (values : seq<'T>, container : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let fileName = fileStore.CreateUniqueFileName container
        let length = ref 0
        let! file = fileStore.CreateFile(fileName, fun stream -> async { return length := serializer.SeqSerialize(stream, values) })
        return new CloudSeq<'T>(fileName, !length, file, serializer)
    }


namespace Nessos.MBrace.Store

open Nessos.MBrace

[<AutoOpen>]
module CloudSeqUtils =

    type ICloudFileStore with
        /// <summary>
        ///     Creates a new CloudSeq instance
        /// </summary>
        /// <param name="values">Values to be serialized.</param>
        /// <param name="container">FileStore container used for cloud sequence.</param>
        /// <param name="serializer">Serializer used for objects.</param>
        member fs.CreateCloudSeq<'T>(values : seq<'T>, container : string, serializer) =
            CloudSeq<'T>.Create(values, container, fs, serializer)