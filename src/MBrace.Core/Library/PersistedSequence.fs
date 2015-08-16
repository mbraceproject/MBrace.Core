namespace MBrace.Library

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library.CloudCollectionUtils

#nowarn "444"

/// <summary>
///     Ordered, immutable collection of values persisted in a single cloud file.
/// </summary>
[<DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type PersistedSequence<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Store")>]
    val mutable private store : ICloudFileStore
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "ETag")>]
    val mutable private etag : ETag
    [<DataMember(Name = "Count")>]
    val mutable private count : int64 option
    [<DataMember(Name = "Deserializer")>]
    val mutable private deserializer : (Stream -> seq<'T>)

    internal new (store, path, etag, count, deserializer) =
        { store = store ; path = path ; etag = etag ; count = count ; deserializer = deserializer }

    member private c.GetEnumerableAsync() : Async<seq<'T>> = async {
        let! streamOpt = c.store.ReadETag(c.path, c.etag)
        return
            match streamOpt with
            | None -> raise <| new InvalidDataException(sprintf "CloudSequence: incorrect etag in file '%s'." c.path)
            | Some stream -> c.deserializer stream
    }

    interface IEnumerable<'T> with
        member c.GetEnumerator(): IEnumerator = Async.RunSync(c.GetEnumerableAsync()).GetEnumerator() :> _
        member c.GetEnumerator(): IEnumerator<'T> = Async.RunSync(c.GetEnumerableAsync()).GetEnumerator()

    /// Asynchronously fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArrayAsync () : Async<'T []> = async { 
        let! seq = c.GetEnumerableAsync()
        return Seq.toArray seq 
    }

    /// Fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArray () : 'T [] = c.ToArrayAsync() |> Async.RunSync

    /// Path to Cloud sequence in store.
    member c.Path = c.path

    /// Gets the underlying ICloudFileStore instance used for the persisted sequence.
    member internal c.Store = c.store

    /// ETag of persisted sequence instance.
    member c.ETag = c.etag

    /// Asynchronously gets Cloud sequence element count
    member c.GetCountAsync() = async {
        match c.count with
        | Some l -> return l
        | None ->
            // this is a potentially costly operation
            let! seq = c.GetEnumerableAsync()
            let l = int64 <| Seq.length seq
            c.count <- Some l
            return l
    }

    /// Gets Cloud sequence element count
    member c.Count = c.GetCountAsync() |> Async.RunSynchronously

    /// Asynchronously gets underlying sequence size in bytes
    member c.GetSizeAsync() = async {
        return! c.store.GetFileSize c.path
    }

    /// Gets underlying sequence size in bytes
    member c.Size = c.GetSizeAsync() |> Async.RunSynchronously

    interface ICloudDisposable with
        member c.Dispose () = async {
            return! c.store.DeleteFile c.path
        }

    interface ICloudCollection<'T> with
        member c.IsKnownCount = Option.isSome c.count
        member c.IsKnownSize = true
        member c.IsMaterialized = false
        member c.GetCount() = c.GetCountAsync()
        member c.GetSize() = c.GetSizeAsync()
        member c.ToEnumerable() = c.GetEnumerableAsync()

    override c.ToString() = sprintf "CloudSequence[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()  

/// Partitionable implementation of cloud file line reader
[<Sealed; DataContract>]
type private TextSequenceByLine(store : ICloudFileStore, path : string, etag : ETag, ?encoding : Encoding) =
    inherit PersistedSequence<string>(store, path, etag, None, (fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding, disposeStream = true)))

    interface IPartitionableCollection<string> with
        member cs.GetPartitions(weights : int []) = async {
            let! size = (cs :> PersistedSequence<_>).GetSizeAsync()

            let mkRangedSeqs (weights : int[]) =
                let getDeserializer s e stream = TextReaders.ReadLinesRanged(stream, max (s - 1L) 0L, e, ?encoding = encoding, disposeStream = true)
                let mkRangedSeq rangeOpt =
                    match rangeOpt with
                    | Some(s,e) -> new PersistedSequence<string>(cs.Store, cs.Path, cs.ETag, None, (getDeserializer s e)) :> ICloudCollection<string>
                    | None -> new ConcatenatedCollection<string>([||]) :> _

                let partitions = Array.splitWeightedRange weights 0L size
                Array.map mkRangedSeq partitions

            if size < 512L * 1024L then
                // partition lines in-memory if file is particularly small.
                let! count = (cs :> PersistedSequence<_>).GetCountAsync()
                if count < int64 weights.Length then
                    let! lines = (cs :> PersistedSequence<_>).ToArrayAsync()
                    let liness = Array.splitWeighted weights lines
                    return liness |> Array.map (fun lines -> new SequenceCollection<string>(lines) :> ICloudCollection<_>)
                else
                    return mkRangedSeqs weights
            else
                return mkRangedSeqs weights
        }

type PersistedSequence =

    /// <summary>
    ///     Creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member New(values : seq<'T>, ?path : string, ?serializer : ISerializer) : Local<PersistedSequence<'T>> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let path = 
            match path with
            | Some p -> p
            | None -> store.GetRandomFilePath store.DefaultDirectory

        let! _serializer = local {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let deserializer (stream : Stream) = _serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        let writer (stream : Stream) = async {
            return _serializer.SeqSerialize<'T>(stream, values, leaveOpen = false) |> int64
        }
        let! etag, length = store.WriteETag(path, writer)
        return new PersistedSequence<'T>(store, path, etag, Some length, deserializer)
    }

    /// <summary>
    ///     Creates a collection of partitioned cloud sequences by persisting provided sequence as cloud files in the underlying store.
    ///     A new partition will be appended to the collection as soon as the 'maxPartitionSize' is exceeded in bytes.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per cloud sequence partition.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member NewPartitioned(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer) : Local<PersistedSequence<'T> []> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let directory = defaultArg directory store.DefaultDirectory
        let! _serializer = local {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let deserializer (stream : Stream) = _serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        return! async {
            if maxPartitionSize <= 0L then return invalidArg "maxPartitionSize" "Must be greater that 0."

            let seqs = new ResizeArray<PersistedSequence<'T>>()
            let currentStream = ref Unchecked.defaultof<Stream>
            let splitNext () = currentStream.Value.Position >= maxPartitionSize
            let partitionedValues = PartitionedEnumerable.ofSeq splitNext values
            for partition in partitionedValues do
                let path = store.GetRandomFilePath directory
                let writer (stream : Stream) = async {
                    currentStream := stream
                    return _serializer.SeqSerialize<'T>(stream, partition, leaveOpen = false) |> int64
                }
                let! etag, length = store.WriteETag(path, writer)
                let seq = new PersistedSequence<'T>(store, path, etag, Some length, deserializer)
                seqs.Add seq

            return seqs.ToArray()
        }
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, ?deserializer : Stream -> seq<'T>, ?force : bool) : Local<PersistedSequence<'T>> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! deserializer = local {
            match deserializer with
            | Some d -> return d
            | None ->
                let! serializer = Cloud.GetResource<ISerializer>()
                return fun s -> serializer.SeqDeserialize<'T>(s, leaveOpen = true)
        }

        let! etag = store.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let cseq = new PersistedSequence<'T>(store, path, et, None, deserializer)
            if defaultArg force false then
                let! _ = cseq.GetCountAsync() in ()

            return cseq
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer implementation used for element deserialization.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool) : Local<PersistedSequence<'T>> = local {
        let deserializer stream = serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        return! PersistedSequence.OfCloudFile<'T>(path, deserializer = deserializer, ?force = force)
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, ?encoding : Encoding, ?force : bool) : Local<PersistedSequence<'T>> = local {
        let deserializer (stream : Stream) =
            let sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 
        
        return! PersistedSequence.OfCloudFile<'T>(path, deserializer, ?force = force)
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFileByLine(path : string, ?encoding : Encoding, ?force : bool) : Local<PersistedSequence<string>> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! etag = store.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let cseq = new TextSequenceByLine(store, path, et, ?encoding = encoding)
            if defaultArg force false then
                let! _ = cseq.GetCountAsync() in ()

            return cseq :> PersistedSequence<string>
    }