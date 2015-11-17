namespace MBrace.Library

open System
open System.Collections
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.Serialization
open System.Text
open System.IO
open System.IO.Compression

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
        let! stream = async {
            match c.etag with
            | null -> return! c.store.BeginRead c.path
            | etag ->
                let! streamOpt = c.store.ReadETag(c.path, etag)
                return
                    match streamOpt with
                    | None -> raise <| new InvalidDataException(sprintf "PersistedSequence: incorrect etag in file '%s'." c.path)
                    | Some stream -> stream
        }

        return c.deserializer stream
    }

    /// Asynchronously fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArrayAsync () : Async<'T []> = async { 
        let! seq = c.GetEnumerableAsync()
        return Seq.toArray seq 
    }

    /// Path to Cloud sequence in store.
    member c.Path = c.path

    /// Gets the underlying ICloudFileStore instance used for the persisted sequence.
    member internal c.Store = c.store

    /// ETag of persisted sequence instance.
    member c.ETag = c.etag

    /// Asynchronously gets persisted sequence element count
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

    /// Asynchronously gets underlying sequence size in bytes
    member c.GetSizeAsync() = async {
        return! c.store.GetFileSize c.path
    }

    /// Gets persisted sequence element count
    member c.GetCount() = Cloud.OfAsync <| c.GetCountAsync()

    /// Gets underlying sequence size in bytes
    member c.GetSize() = Cloud.OfAsync <| c.GetSizeAsync()

    /// Gets Cloud sequence element count
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member c.Count = c.GetCountAsync() |> Async.RunSync

    /// Gets underlying sequence size in bytes
    member c.Size = c.GetSizeAsync() |> Async.RunSync

    /// Fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArray () : 'T [] = c.ToArrayAsync() |> Async.RunSync

    interface ICloudDisposable with
        member c.Dispose () = async {
            return! c.store.DeleteFile c.path
        }

    interface IEnumerable<'T> with
        member c.GetEnumerator(): IEnumerator = Async.RunSync(c.GetEnumerableAsync()).GetEnumerator() :> _
        member c.GetEnumerator(): IEnumerator<'T> = Async.RunSync(c.GetEnumerableAsync()).GetEnumerator()

    interface ICloudCollection<'T> with
        member c.IsKnownCount = Option.isSome c.count
        member c.IsKnownSize = true
        member c.IsMaterialized = false
        member c.GetCountAsync() = c.GetCountAsync()
        member c.GetSizeAsync() = c.GetSizeAsync()
        member c.GetEnumerableAsync() = c.GetEnumerableAsync()

    override c.ToString() = sprintf "PersistedSequence[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()  

/// Partitionable implementation of cloud file line reader
[<Sealed; DataContract>]
type private TextSequenceByLine(store : ICloudFileStore, path : string, etag : ETag, ?encoding : Encoding, ?range: (int64 * int64)) =
    inherit PersistedSequence<string>(store, path, etag, None, 
            match range with
            | Some (s, e) -> (fun stream -> TextReaders.ReadLinesRanged(stream, max (s - 1L) 0L, e, ?encoding = encoding, disposeStream = true))
            | None -> (fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding, disposeStream = true)))

    [<DataMember(Name = "Range")>]
    let range = range

    interface IPartitionableCollection<string> with
        member cs.GetPartitions(weights : int []) = async {
            let! size = (cs :> PersistedSequence<_>).GetSizeAsync()

            let mkRangedSeqs (weights : int[]) =
                let mkRangedSeq rangeOpt =
                    match rangeOpt with
                    | Some(s,e) -> new TextSequenceByLine(cs.Store, cs.Path, cs.ETag, ?encoding = encoding, range = (s, e)) :> ICloudCollection<string>
                    | None -> new ConcatenatedCollection<string>([||]) :> _

                let (s, e) = match range with Some (s, e) -> (s, e + 1L) | None -> (0L, size)
                let partitions =  Array.splitWeightedRange weights s e
                Array.map mkRangedSeq partitions

            return mkRangedSeqs weights
        }

/// Persisted Sequence collection of APIs
type PersistedSequence =

    /// <summary>
    ///     Creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    static member New(values : seq<'T>, [<O;D(null:obj)>]?path : string, [<O;D(null:obj)>]?serializer : ISerializer, [<O;D(null:obj)>]?compress : bool) : LocalCloud<PersistedSequence<'T>> = local {
        let compress = defaultArg compress false
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

        let deserializer (stream : Stream) = 
            let stream =
                if compress then new GZipStream(stream, CompressionMode.Decompress) :> Stream
                else stream

            _serializer.SeqDeserialize<'T>(stream, leaveOpen = false)

        let writer (stream : Stream) = async {
            let stream =
                if compress then new GZipStream(stream, CompressionLevel.Optimal) :> Stream
                else stream

            return _serializer.SeqSerialize<'T>(stream, values, leaveOpen = false) |> int64
        }
        let! etag, length = Cloud.OfAsync <| store.WriteETag(path, writer)
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
    static member NewPartitioned(values : seq<'T>, maxPartitionSize : int64, [<O;D(null:obj)>]?directory : string, [<O;D(null:obj)>]?serializer : ISerializer) : LocalCloud<PersistedSequence<'T> []> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let directory = defaultArg directory store.DefaultDirectory
        let! _serializer = local {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let deserializer (stream : Stream) = _serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        return! Cloud.OfAsync <| async {
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
    ///     Defines a PersistedSequence from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="ensureFileExists">Check that path exists in store. Defaults to true.</param>
    /// <param name="forceEvaluation">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, [<O;D(null:obj)>]?deserializer : Stream -> seq<'T>, [<O;D(null:obj)>]?ensureFileExists : bool, [<O;D(null:obj)>]?forceEvaluation : bool, ?resolveEtag : bool) : LocalCloud<PersistedSequence<'T>> = local {
        let ensureFileExists = defaultArg ensureFileExists true
        let forceEvaluation = defaultArg forceEvaluation false
        let resolveEtag = defaultArg resolveEtag false
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! deserializer = local {
            match deserializer with
            | Some d -> return d
            | None ->
                let! serializer = Cloud.GetResource<ISerializer>()
                return fun s -> serializer.SeqDeserialize<'T>(s, leaveOpen = true)
        }

        let! etag = Cloud.OfAsync <| async {
            if resolveEtag then
                let! etag = store.TryGetETag path
                match etag with
                | None -> return raise <| new FileNotFoundException(path)
                | Some et -> return et
            elif not forceEvaluation && ensureFileExists then
                let! exists = store.FileExists path
                if not exists then return raise <| new FileNotFoundException(path)
                else
                    return null        
            else
                return null
        }
        
        let cseq = new PersistedSequence<'T>(store, path, etag, None, deserializer)
        if forceEvaluation then let! _ = Cloud.OfAsync <| cseq.GetCountAsync() in ()

        return cseq
    }

    /// <summary>
    ///     Defines a PersistedSequence from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer implementation used for element deserialization.</param>
    /// <param name="ensureFileExists">Check that path exists in store. Defaults to true.</param>
    /// <param name="forceEvaluation">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, [<O;D(null:obj)>]?ensureFileExists : bool, [<O;D(null:obj)>]?forceEvaluation : bool, [<O;D(null:obj)>]?resolveEtag : bool) : LocalCloud<PersistedSequence<'T>> = local {
        let deserializer stream = serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        return! PersistedSequence.OfCloudFile<'T>(path, deserializer = deserializer, ?ensureFileExists = ensureFileExists, ?forceEvaluation = forceEvaluation, ?resolveEtag = resolveEtag)
    }

    /// <summary>
    ///     Defines a PersistedSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="ensureFileExists">Check that path exists in store. Defaults to true.</param>
    /// <param name="forceEvaluation">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, [<O;D(null:obj)>]?ensureFileExists : bool, [<O;D(null:obj)>]?encoding : Encoding, [<O;D(null:obj)>]?forceEvaluation : bool, [<O;D(null:obj)>]?resolveEtag : bool) : LocalCloud<PersistedSequence<'T>> = local {
        let deserializer (stream : Stream) =
            let sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 
        
        return! PersistedSequence.OfCloudFile<'T>(path, deserializer, ?ensureFileExists = ensureFileExists, ?forceEvaluation = forceEvaluation, ?resolveEtag = resolveEtag)
    }

    /// <summary>
    ///     Defines a PersistedSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="ensureFileExists">Check that path exists in store. Defaults to true.</param>
    /// <param name="forceEvaluation">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFileByLine(path : string, [<O;D(null:obj)>]?encoding : Encoding, [<O;D(null:obj)>]?ensureFileExists : bool, [<O;D(null:obj)>]?forceEvaluation : bool, [<O;D(null:obj)>]?resolveEtag : bool) : LocalCloud<PersistedSequence<string>> = local {
        let ensureFileExists = defaultArg ensureFileExists true
        let forceEvaluation = defaultArg forceEvaluation false
        let resolveEtag = defaultArg resolveEtag false
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! etag = Cloud.OfAsync <| async {
            if resolveEtag then
                let! etag = store.TryGetETag path
                match etag with
                | None -> return raise <| new FileNotFoundException(path)
                | Some et -> return et
            elif not forceEvaluation && ensureFileExists then
                let! exists = store.FileExists path
                if not exists then return raise <| new FileNotFoundException(path)
                else
                    return null        
            else
                return null
        }

        let cseq = new TextSequenceByLine(store, path, etag, ?encoding = encoding)
        if forceEvaluation then let! _ = Cloud.OfAsync <| cseq.GetCountAsync() in ()
        return cseq :> PersistedSequence<string>
    }