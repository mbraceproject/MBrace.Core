namespace MBrace.Library

open System
open System.Diagnostics
open System.Runtime.Serialization
open System.IO
open System.IO.Compression
open System.Text

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals

#nowarn "444"

/// Represents an immutable reference to an
/// object that is persisted as a cloud file.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type PersistedValue<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Store")>]
    val mutable private store : ICloudFileStore
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "ETag")>]
    val mutable private etag : ETag
    [<DataMember(Name = "Deserializer")>]
    val mutable private deserializer : Stream -> 'T

    internal new (store, path, etag, deserializer) =
        { store = store; path = path ; etag = etag ; deserializer = deserializer }

    /// Path to cloud value payload in store
    member c.Path = c.path

    /// Asynchronously dereferences value from store
    member c.GetValueAsync () : Async<'T> = async {
        use! stream = async {
            match c.etag with
            | null -> return! c.store.BeginRead c.path
            | etag ->
                let! streamOpt = c.store.ReadETag(c.path, etag)
                match streamOpt with
                | None -> return raise <| new InvalidDataException(sprintf "PersistedValue: incorrect etag in file '%s'." c.path)
                | Some stream -> return stream
        }

        return c.deserializer stream
    }

    /// Asynchronously gets the size of the persisted value in bytes.
    member c.GetSizeAsync () : Async<int64> = async {
        return! c.store.GetFileSize c.path
    }

    /// Dereferences value from store
    member c.GetValue () : LocalCloud<'T> = Cloud.OfAsync <| c.GetValueAsync()

    /// Gets the size of the persisted value in bytes.
    member c.GetSize () : LocalCloud<int64> = Cloud.OfAsync <| c.GetSizeAsync()

    /// Dereferences value from store.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member c.Value : 'T = c.GetValueAsync() |> Async.RunSync

    /// Gets the size of the persisted value in bytes.
    member c.Size : int64 = c.GetSizeAsync() |> Async.RunSync

    override c.ToString() = sprintf "PersistedValue[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()

    interface ICloudDisposable with
        member c.Dispose () = async {
            return! c.store.DeleteFile c.path
        }

#nowarn "444"

/// PersistedValue extension methods
type PersistedValue =
    
    /// <summary>
    ///     Creates a new PersistedValue by persisting input as a cloud file in the underlying store.
    /// </summary>
    /// <param name="value">Input value.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime serializer.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    static member New(value : 'T, ?path : string, ?serializer : ISerializer, ?compress : bool) : LocalCloud<PersistedValue<'T>> = local {
        let compress = defaultArg compress false
        let! store = Cloud.GetResource<ICloudFileStore>()
        let path = 
            match path with
            | Some p -> p
            | None -> store.GetRandomFilePath store.DefaultDirectory

        let! _serializer = local {
            match serializer with 
            | Some s -> return s 
            | None -> return! Cloud.GetResource<ISerializer>()
        }

        let deserializer (stream : Stream) = 
            let stream =
                if compress then new GZipStream(stream, CompressionMode.Decompress) :> Stream
                else stream
  
            _serializer.Deserialize<'T>(stream, leaveOpen = false)

        let writer (stream : Stream) = async {
            let stream = 
                if compress then new GZipStream(stream, CompressionLevel.Optimal) :> Stream
                else stream

            _serializer.Serialize(stream, value, leaveOpen = false)
        }

        let! etag,_ = Cloud.OfAsync <| store.WriteETag(path, writer)
        return new PersistedValue<'T>(store, path, etag, deserializer)
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool, ?resolveEtag : bool) : LocalCloud<PersistedValue<'T>> = local {
        let force = defaultArg force false
        let resolveEtag = defaultArg resolveEtag false
        let! store = Cloud.GetResource<ICloudFileStore>()
        let! deserializer = local {
            match deserializer with 
            | Some d -> return d
            | None -> 
                let! serializer = Cloud.GetResource<ISerializer>()
                return fun s -> serializer.Deserialize<'T>(s, leaveOpen = false)
        }

        let! etag = Cloud.OfAsync <| async {
            if resolveEtag then
                let! etag = store.TryGetETag path
                match etag with
                | None -> return raise <| new FileNotFoundException(path)
                | Some et -> return et
            elif not force then
                let! exists = store.FileExists path
                if not exists then raise <| new FileNotFoundException(path)
                return null
            else
                return null
        }

        let fpv = new PersistedValue<'T>(store, path, etag, deserializer)
        if force then let! _ = Cloud.OfAsync <| fpv.GetValueAsync() in ()
        return fpv
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer implementation used for value.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool, ?resolveEtag : bool) = local {
        let deserializer stream = serializer.Deserialize<'T>(stream, leaveOpen = false)
        return! PersistedValue.OfCloudFile<'T>(path, deserializer = deserializer, ?force = force, ?resolveEtag = resolveEtag)
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided text deserializer and encoding.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="resolveEtag">Determine the CloudFile ETag and pass it to the PersistedSequence. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> 'T, ?encoding : Encoding, ?force : bool, ?resolveEtag : bool) : LocalCloud<PersistedValue<'T>> = local {
        let deserializer (stream : Stream) =
            let sr =
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 

        return! PersistedValue.OfCloudFile(path, deserializer, ?force = force, ?resolveEtag = resolveEtag)
    }