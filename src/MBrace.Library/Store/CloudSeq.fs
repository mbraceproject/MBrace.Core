namespace Nessos.MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

type private CloudSeqHeader = { Type : Type ; Count : int ; Payload : string }

/// Represents a finite and immutable sequence of
/// elements that is stored in the underlying CloudStore
/// and will be enumerated on demand.
[<Sealed; DataContract>]
type CloudSeq<'T> =

    // https://visualfsharp.codeplex.com/workitem/199

    [<DataMember(Name = "HeaderPath")>]
    val mutable private path : string
    [<DataMember(Name = "PayloadPath")>]
    val mutable private payloadPath : string
    [<DataMember(Name = "Count")>]
    val mutable private count : int
    [<DataMember(Name = "FileStore")>]
    val mutable private fileStore : ICloudFileStore
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer

    private new (path, count, payloadPath, fileStore, serializer) = 
        { path = path ; count = count ; payloadPath = payloadPath ; fileStore = fileStore ; serializer = serializer }

    /// Asynchronously fetches the sequence
    member c.GetSequenceAsync() = async {
        match c.payloadPath with
        | null ->
            // header and payload found on same file
            let! stream = c.fileStore.BeginRead(c.path)
            let _ = c.serializer.Deserialize<CloudSeqHeader>(stream, leaveOpen = true)
            return c.serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        | path ->
            // payload found on different file
            let! stream = c.fileStore.BeginRead(path)
            return c.serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
    }

    /// Path to Cloud sequence in store
    member c.Path = c.path
    /// Cloud sequence element count
    member c.Count = c.count

    interface ICloudStorageEntity with
        member c.Type = sprintf "cloudseq:%O" typeof<'T>
        member c.Id = c.path

    interface ICloudDisposable with
        member c.Dispose () = async {
            do! c.fileStore.DeleteFile c.path
            match c.payloadPath with
            | null -> ()
            | p -> do! c.fileStore.DeleteFile p 
        }

    interface IEnumerable<'T> with
        member c.GetEnumerator() = (c.GetSequenceAsync() |> Async.RunSync :> IEnumerable).GetEnumerator()
        member c.GetEnumerator() = (c.GetSequenceAsync() |> Async.RunSync).GetEnumerator()

    /// <summary>
    ///     Creates a new cloud sequence with given values in provided file store.
    /// </summary>
    /// <param name="values">Sequence to be serialized.</param>
    /// <param name="directory">Containing directory in file store.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member Create (values : seq<'T>, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let fileName = fileStore.GetRandomFilePath directory
        use! stream = fileStore.BeginWrite fileName
        // use different encodings depending on stream ability to seek
        if stream.CanSeek then
            // serialize initial dummy header
            let header = { Type = typeof<'T> ; Count = 0 ; Payload = null }
            serializer.Serialize(stream, header, leaveOpen = true)
            // serialize sequence, extract element count
            let count = serializer.SeqSerialize(stream, values, leaveOpen = true)
            // move to origin of stream, serializing element count at the header
            let _ = stream.Seek(0L, SeekOrigin.Begin)
            serializer.Serialize(stream, { header with Count = count}, leaveOpen = false)
            return new CloudSeq<'T>(fileName, count, null, fileStore, serializer)
        else
            let payloadPath = fileStore.GetRandomFilePath directory
            use! stream = fileStore.BeginWrite payloadPath
            let count = serializer.SeqSerialize(stream, values, leaveOpen = false)
            let header = { Type = typeof<'T> ; Count = count ; Payload = payloadPath }
            use! stream' = fileStore.BeginWrite fileName
            in serializer.Serialize(stream', header, leaveOpen = false)
            return new CloudSeq<'T>(fileName, count, payloadPath, fileStore, serializer)
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to cloud sequence.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member Parse(path : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        use! stream = fileStore.BeginRead path
        let header = serializer.Deserialize<CloudSeqHeader>(stream, leaveOpen = false)
        return
            if header.Type = typeof<'T> then
                new CloudSeq<'T>(path, header.Count, header.Payload, fileStore, serializer)
            else
                let msg = sprintf "expected cloudseq of type %O but was %O." typeof<'T> header.Type
                raise <| new InvalidDataException(msg)
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
        return! Cloud.OfAsync <| CloudSeq<'T>.Create(values, path, config.FileStore, serializer)
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member Parse<'T>(path : string, ?serializer) : Cloud<CloudSeq<'T>> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        return! Cloud.OfAsync <| CloudSeq<'T>.Parse(path, config.FileStore, serializer)
    }