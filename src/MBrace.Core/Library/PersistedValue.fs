namespace MBrace.Library

open System
open System.Runtime.Serialization
open System.IO
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
        let! streamOpt = c.store.ReadETag(c.path, c.etag)
        match streamOpt with
        | None -> return raise <| new InvalidDataException(sprintf "CloudValue: incorrect etag in file '%s'." c.path)
        | Some stream -> use stream = stream in return c.deserializer stream
    }

    /// Asynchronously gets the size of the persisted value in bytes.
    member c.GetSizeAsync () : Async<int64> = async {
        return! c.store.GetFileSize c.path
    }

    /// Dereferences value from store.
    member c.Value : 'T = c.GetValueAsync() |> Async.RunSync

    /// Gets the size of the persisted value in bytes.
    member c.Size : int64 = c.GetSizeAsync() |> Async.RunSync

    override c.ToString() = sprintf "CloudValue[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()

    interface ICloudDisposable with
        member c.Dispose () = async {
            return! c.store.DeleteFile c.path
        }

#nowarn "444"

type PersistedValue =
    
    /// <summary>
    ///     Creates a new PersistedValue by persisting input as a cloud file in the underlying store.
    /// </summary>
    /// <param name="value">Input value.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime serializer.</param>
    static member New(value : 'T, ?path : string, ?serializer : ISerializer) : Local<PersistedValue<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let path = 
            match path with
            | Some p -> p
            | None -> config.FileStore.GetRandomFilePath config.DefaultDirectory

        let! _serializer = local {
            match serializer with 
            | Some s -> return s 
            | None -> return! Cloud.GetResource<ISerializer>()
        }

        let deserializer (stream : Stream) = _serializer.Deserialize<'T>(stream, leaveOpen = false)
        let writer (stream : Stream) = async {
            // write value
            _serializer.Serialize(stream, value, leaveOpen = false)
        }

        let! etag,_ = config.FileStore.WriteETag(path, writer)
        return new PersistedValue<'T>(config.FileStore, path, etag, deserializer)
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool) : Local<PersistedValue<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let! deserializer = local {
            match deserializer with 
            | Some d -> return d
            | None -> 
                let! serializer = Cloud.GetResource<ISerializer>()
                return fun s -> serializer.Deserialize<'T>(s, leaveOpen = false)
        }

        let! etag = config.FileStore.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let fpv = new PersistedValue<'T>(config.FileStore, path, et, deserializer)
            if defaultArg force false then
                let! _ = fpv.GetValueAsync() in ()
            return fpv
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer implementation used for value.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool) = local {
        let deserializer stream = serializer.Deserialize<'T>(stream, leaveOpen = false)
        return! PersistedValue.OfCloudFile<'T>(path, deserializer = deserializer, ?force = force)
    }

    /// <summary>
    ///     Defines a PersistedValue from provided cloud file path with user-provided text deserializer and encoding.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> 'T, ?encoding : Encoding, ?force : bool) : Local<PersistedValue<'T>> = local {
        let deserializer (stream : Stream) =
            let sr =
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 

        return! PersistedValue.OfCloudFile(path, deserializer, ?force = force)
    }

    /// <summary>
    ///     Dereferences a persisted value.
    /// </summary>
    /// <param name="cloudCell">CloudValue to be dereferenced.</param>
    static member Read(value : PersistedValue<'T>) : Local<'T> = local { return! value.GetValueAsync() }