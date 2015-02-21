namespace MBrace

open System
open System.Runtime.Serialization
open System.IO

open MBrace
open MBrace.Store
open MBrace.Continuation

#nowarn "444"

type private CloudRefHeader = { Type : Type ; UUID : string }

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references cached locally for performance.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudRef<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "UUID")>]
    val mutable private uuid : string
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer option

    internal new (uuid, path, serializer) =
        { path = path ; uuid = uuid ; serializer = serializer }

    member private r.GetValueFromStore(config : CloudFileStoreConfiguration) = async {
        let serializer = match r.serializer with Some s -> s | None -> config.Serializer
        use! stream = config.FileStore.BeginRead r.path
        // consume header
        let _ = serializer.Deserialize<CloudRefHeader>(stream, leaveOpen = true)
        // deserialize payload
        return serializer.Deserialize<'T>(stream, leaveOpen = false)
    }

    /// Path to cloud ref payload in store
    member r.Path = r.path

    /// Dereference the cloud ref
    member r.Value = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        match config.Cache |> Option.bind (fun c -> c.TryFind r.uuid) with
        | Some v -> return v :?> 'T
        | None -> return! ofAsync <| r.GetValueFromStore(config)
    }

    /// Caches the cloud ref value to the local execution contexts. Returns true iff successful.
    member r.Cache() = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        match config.Cache with
        | None -> return false
        | Some c ->
            if c.ContainsKey r.uuid then return true
            else
                let! v = ofAsync <| r.GetValueFromStore(config)
                return c.Add(r.uuid, v)
    }

    /// Indicates if array is cached in local execution context
    member c.IsCachedLocally = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.Cache |> Option.exists(fun ch -> ch.ContainsKey c.uuid)
    }

    /// Gets the size of cloud ref in bytes
    member r.Size = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        return! ofAsync <| config.FileStore.GetFileSize r.path
    }

    override r.ToString() = sprintf "CloudRef[%O] at %s" typeof<'T> r.path
    member private r.StructuredFormatDisplay = r.ToString()

    interface ICloudDisposable with
        member r.Dispose () = cloud {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
            return! ofAsync <| config.FileStore.DeleteFile r.path
        }

    interface ICloudStorageEntity with
        member r.Type = sprintf "cloudref:%O" typeof<'T>
        member r.Id = r.path

#nowarn "444"

type CloudRef =
    
    /// <summary>
    ///     Creates a new cloud reference to the underlying store with provided value.
    ///     Cloud references are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud reference value.</param>
    /// <param name="directory">FileStore directory used for cloud ref. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    static member New(value : 'T, ?directory : string, ?serializer : ISerializer) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let directory = defaultArg directory config.DefaultDirectory
        let _serializer = match serializer with Some s -> s | None -> config.Serializer
        return! ofAsync <| async {
            let uuid = Guid.NewGuid().ToString()
            let path = config.FileStore.GetRandomFilePath directory
            let writer (stream : Stream) = async {
                // write header
                _serializer.Serialize(stream, { Type = typeof<'T> ; UUID = uuid }, leaveOpen = true)
                // write value
                _serializer.Serialize(stream, value, leaveOpen = false)
            }
            do! config.FileStore.Write(path, writer)
            return new CloudRef<'T>(uuid, path, serializer)
        }
    }

    /// <summary>
    ///     Parses a cloud ref of given type with provided serializer. If successful, returns the cloud ref instance.
    /// </summary>
    /// <param name="path">Path to cloud ref.</param>
    /// <param name="serializer">Serializer for cloud ref.</param>
    static member Parse<'T>(path : string, ?serializer : ISerializer) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let _serializer = match serializer with Some s -> s | None -> config.Serializer
        return! ofAsync <| async {
            use! stream = config.FileStore.BeginRead path
            let header = 
                try _serializer.Deserialize<CloudRefHeader>(stream, leaveOpen = false)
                with e -> raise <| new FormatException("Error reading cloud ref header.", e)
            return
                if header.Type = typeof<'T> then
                    new CloudRef<'T>(header.UUID, path, serializer)
                else
                    let msg = sprintf "expected cloudref of type %O but was %O." typeof<'T> header.Type
                    raise <| new InvalidDataException(msg)
        }
    }

    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    static member Read(cloudRef : CloudRef<'T>) : Cloud<'T> = cloudRef.Value

    /// <summary>
    ///     Cache a cloud reference to local execution context
    /// </summary>
    /// <param name="cloudRef">Cloud ref input</param>
    static member Cache(cloudRef : CloudRef<'T>) : Cloud<bool> = cloudRef.Cache()