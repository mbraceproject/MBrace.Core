namespace MBrace

open System
open System.Runtime.Serialization
open System.IO
open System.Text

open MBrace
open MBrace.Store
open MBrace.Continuation

#nowarn "444"

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud cells cached locally for performance.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudValue<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "UUID")>]
    val mutable private uuid : string
    [<DataMember(Name = "Deserializer")>]
    val mutable private deserializer : (Stream -> 'T) option
    [<DataMember(Name = "IsCacheEnabled")>]
    val mutable private isCacheEnabled : bool

    internal new (path, deserializer, ?enableCache) =
        let uuid = Guid.NewGuid().ToString()
        let enableCache = defaultArg enableCache true
        { uuid = uuid ; path = path ; deserializer = deserializer ; isCacheEnabled = enableCache }

    /// Path to cloud value payload in store
    member c.Path = c.path

    /// Enables or disables caching setting for current cloud value instance.
    member c.EnableCache 
        with get () = c.isCacheEnabled
        and set ch = c.isCacheEnabled <- ch

    interface ICloudCacheable<'T> with
        member c.UUID = c.uuid
        member c.GetSourceValue() = local {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
            use! stream = ofAsync <| config.FileStore.BeginRead c.path
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
    ///     Creates a new local reference to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud value value.</param>
    /// <param name="directory">FileStore directory used for cloud value. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    /// <param name="enableCache">Enables implicit, on-demand caching of cell value across instances. Defaults to true.</param>
    static member New(value : 'T, ?directory : string, ?serializer : ISerializer, ?enableCache : bool) = local {
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
        do! ofAsync <| config.FileStore.Write(path, writer)
        return new CloudValue<'T>(path, deserializer, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Creates a CloudValue from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Value deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member FromFile<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool, ?enableCache : bool) : Local<CloudValue<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let cell = new CloudValue<'T>(path, deserializer, ?enableCache = enableCache)
        if defaultArg force false then
            let! _ = cell.Value in ()
        else
            let! exists = ofAsync <| config.FileStore.FileExists path
            if not exists then return raise <| new FileNotFoundException(path)
        return cell
    }

    /// <summary>
    ///     Creates a cloud value of given type with provided serializer. If successful, returns the cloud value instance.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer for cloud value.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member FromFile<'T>(path : string, ?serializer : ISerializer, ?force : bool, ?enableCache : bool) = local {
        let deserializer = serializer |> Option.map (fun ser stream -> ser.Deserialize<'T>(stream, leaveOpen = false))
        return! CloudValue.FromFile<'T>(path, ?deserializer = deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Creates a CloudValue parsing a text file.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    static member FromTextFile<'T>(path : string, textDeserializer : StreamReader -> 'T, ?encoding : Encoding, ?force : bool, ?enableCache) : Local<CloudValue<'T>> = local {
        let deserializer (stream : Stream) =
            let sr =
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 

        return! CloudValue.FromFile(path, deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Dereference a Cloud value.
    /// </summary>
    /// <param name="cloudCell">CloudValue to be dereferenced.</param>
    static member Read(cloudCell : CloudValue<'T>) : Local<'T> = cloudCell.Value

    /// <summary>
    ///     Disposes cloud value from store.
    /// </summary>
    /// <param name="cloudCell">Cloud value to be deleted.</param>
    static member Dispose(cloudCell : CloudValue<'T>) : Local<unit> = local { return! (cloudCell :> ICloudDisposable).Dispose() }