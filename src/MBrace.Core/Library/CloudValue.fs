namespace MBrace.Store

open System
open System.Runtime.Serialization
open System.IO
open System.Text

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store.Internals

#nowarn "444"

/// Represents an immutable reference to an
/// object that is persisted as a cloud file.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudValue<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "ETag")>]
    val mutable private etag : ETag
    [<DataMember(Name = "Deserializer")>]
    val mutable private deserializer : (Stream -> 'T) option
    [<DataMember(Name = "IsCacheEnabled")>]
    val mutable private isCacheEnabled : bool

    internal new (path, etag, deserializer, ?enableCache) =
        let enableCache = defaultArg enableCache true
        { path = path ; etag = etag ; deserializer = deserializer ; isCacheEnabled = enableCache }

    /// Path to cloud value payload in store
    member c.Path = c.path

    /// Enables or disables caching setting for current cloud value instance.
    member c.EnableCache = c.isCacheEnabled

    /// immutable update to the cache behaviour
    member internal c.WithCacheBehaviour b = new CloudValue<'T>(c.path, c.etag, c.deserializer, b)

    interface ICloudCacheable<'T> with
        member c.UUID = sprintf "CloudValue:%s:%s" c.path c.etag
        member c.GetSourceValue() = local {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
            let! streamOpt = ofAsync <| config.FileStore.TryBeginRead(c.path, c.etag)
            match streamOpt with
            | None -> return raise <| new InvalidDataException(sprintf "CloudValue: incorrect etag in file '%s'." c.path)
            | Some stream ->
                use stream = stream
                match c.deserializer with 
                | Some ds -> return ds stream
                | None -> 
                    let! defaultSerializer = Cloud.GetResource<ISerializer> ()
                    return defaultSerializer.Deserialize<'T>(stream, leaveOpen = false)
        }

    /// Dereference the cloud value
    member c.Value = local { return! CloudCache.GetCachedValue(c, cacheIfNotExists = c.isCacheEnabled) }

    /// Force caching of value to local execution context. Returns true if succesful.
    member c.ForceCache () = local { return! CloudCache.PopulateCache c }

    /// Indicates if array is cached in local execution context
    member c.IsCachedLocally = local { return! CloudCache.IsCachedEntity c }

    /// Gets the size of cloud value in bytes
    member c.Size = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        return! ofAsync <| config.FileStore.GetFileSize c.path
    }

    override c.ToString() = sprintf "CloudValue[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()

    interface ICloudDisposable with
        member c.Dispose () = local {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
            return! ofAsync <| config.FileStore.DeleteFile c.path
        }

    interface ICloudStorageEntity with
        member c.Type = sprintf "cloudref:%O" typeof<'T>
        member c.Id = c.path

#nowarn "444"

type CloudValue =
    
    /// <summary>
    ///     Creates a new CloudValue by persisting input as a cloud file in the underlying store.
    /// </summary>
    /// <param name="value">Input value.</param>
    /// <param name="directory">FileStore directory used for cloud value. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime serializer.</param>
    /// <param name="enableCache">Enables implicit, on-demand caching of cell value across instances. Defaults to true.</param>
    static member New(value : 'T, ?directory : string, ?serializer : ISerializer, ?enableCache : bool) : Local<CloudValue<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let directory = defaultArg directory config.DefaultDirectory
        let! _serializer = local {
            match serializer with 
            | Some s -> return s 
            | None -> return! Cloud.GetResource<ISerializer>()
        }

        let deserializer = serializer |> Option.map (fun ser stream -> ser.Deserialize<'T>(stream, leaveOpen = false))
        let path = config.FileStore.GetRandomFilePath directory
        let writer (stream : Stream) = async {
            // write value
            _serializer.Serialize(stream, value, leaveOpen = false)
        }
        let! etag,_ = ofAsync <| config.FileStore.Write(path, writer)
        return new CloudValue<'T>(path, etag, deserializer, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member OfCloudFile<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool, ?enableCache : bool) : Local<CloudValue<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let! etag = ofAsync <| config.FileStore.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let cell = new CloudValue<'T>(path, et, deserializer, ?enableCache = enableCache)
            if defaultArg force false then
                let! _ = cell.Value in ()
            return cell
    }

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer implementation used for value.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache : bool) = local {
        let deserializer stream = serializer.Deserialize<'T>(stream, leaveOpen = false)
        return! CloudValue.OfCloudFile<'T>(path, deserializer = deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided text deserializer and encoding.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> 'T, ?encoding : Encoding, ?force : bool, ?enableCache) : Local<CloudValue<'T>> = local {
        let deserializer (stream : Stream) =
            let sr =
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 

        return! CloudValue.OfCloudFile(path, deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Dereferences a Cloud value.
    /// </summary>
    /// <param name="cloudCell">CloudValue to be dereferenced.</param>
    static member Read(cloudCell : CloudValue<'T>) : Local<'T> = cloudCell.Value

    /// <summary>
    ///     Creates a copy of CloudValue with updated cache behaviour.
    /// </summary>
    /// <param name="cacheByDefault">Cache behaviour to be set.</param>
    /// <param name="cv">Input cloud value.</param>
    static member WithCacheBehaviour (cacheByDefault:bool) (cv:CloudValue<'T>) : CloudValue<'T> = cv.WithCacheBehaviour cacheByDefault