namespace MBrace

open System
open System.Runtime.Serialization
open System.IO

open MBrace
open MBrace.Store
open MBrace.Continuation

type private CloudRefHeader = { Type : Type ; UUID : string }

type private CloudRefCache =
    
    static member inline internal Add<'T>(uuid, value : 'T) =
        match InMemoryCacheRegistry.InstalledCache with
        | None -> ()
        | Some c -> c.TryAdd(uuid, value) |> ignore

    static member inline internal TryFind<'T>(uuid) =
        match InMemoryCacheRegistry.InstalledCache with
        | None -> None
        | Some c -> c.TryFind<'T>(uuid)

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references cached locally for performance.
[<Sealed; DataContract>]
type CloudRef<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "UUID")>]
    val mutable private uuid : string
    [<DataMember(Name = "FileStore")>]
    val mutable private fileStore : ICloudFileStore
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer

    private new (uuid, path, fileStore, serializer) =
        { path = path ; uuid = uuid ; fileStore = fileStore ; serializer = serializer }

    /// Asynchronously dereferences the cloud ref.
    member r.GetValue () = async {
        match CloudRefCache.TryFind r.uuid with
        | Some v -> return v
        | None ->
            use! stream = r.fileStore.BeginRead r.path
            let _ = r.serializer.Deserialize<CloudRefHeader>(stream, leaveOpen = true)
            let v = r.serializer.Deserialize<'T>(stream, leaveOpen = false)
            CloudRefCache.Add(r.uuid, v)
            return v
    }

    /// Synchronously dereferences the cloud ref.
    member r.Value =
        match CloudRefCache.TryFind r.uuid with
        | Some v -> v
        | None -> r.GetValue() |> Async.RunSync

    /// Returns size of cloud ref in bytes
    member r.Size = r.fileStore.GetFileSize r.path |> Async.RunSync

    interface ICloudDisposable with
        member r.Dispose () = r.fileStore.DeleteFile r.path

    interface ICloudStorageEntity with
        member r.Type = sprintf "cloudref:%O" typeof<'T>
        member r.Id = r.path

    /// <summary>
    ///     Creates a new cloud ref in underlying file store with given serializer.
    /// </summary>
    /// <param name="value">Value to be stored in cloud ref.</param>
    /// <param name="directory">Containing directory in file store.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member Create(value : 'T, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let uuid = Guid.NewGuid().ToString()
        let path = fileStore.GetRandomFilePath directory
        use! stream = fileStore.BeginWrite path 
        serializer.Serialize(stream, { Type = typeof<'T> ; UUID = uuid }, leaveOpen = true)
        serializer.Serialize(stream, value, leaveOpen = false)
        return new CloudRef<'T>(uuid, path, fileStore, serializer)
    }

    /// <summary>
    ///     Parses a cloud ref of given type with provided serializer. If successful, returns the cloud ref instance.
    /// </summary>
    /// <param name="path">Path to cloud ref.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member Parse(path : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        use! stream = fileStore.BeginRead path
        let header = 
            try serializer.Deserialize<CloudRefHeader>(stream, leaveOpen = false)
            with e -> raise <| new FormatException("Error reading cloud ref header.", e)
        return
            if header.Type = typeof<'T> then
                new CloudRef<'T>(header.UUID, path, fileStore, serializer)
            else
                let msg = sprintf "expected cloudref of type %O but was %O." typeof<'T> header.Type
                raise <| new InvalidDataException(msg)
    }

#nowarn "444"

type CloudRef =
    
    /// <summary>
    ///     Creates a new cloud reference to the underlying store with provided value.
    ///     Cloud references are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud reference value.</param>
    /// <param name="directory">FileStore directory used for cloud ref. Defaults to execution context setting.</param>
    /// <param name="serializer">Serialization used for object serialization. Defaults to runtime context.</param>
    static member New(value : 'T, ?directory : string, ?serializer : ISerializer) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration>()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let directory = match directory with None -> fs.DefaultDirectory | Some d -> d

        return! Cloud.OfAsync <| CloudRef<'T>.Create(value, directory, fs.FileStore, serializer)
    }

    /// <summary>
    ///     Parses a cloud ref of given type with provided serializer. If successful, returns the cloud ref instance.
    /// </summary>
    /// <param name="path">Path to cloud ref.</param>
    /// <param name="serializer">Serializer for cloud ref.</param>
    static member Parse<'T>(path : string, ?serializer : ISerializer) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration>()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        return! Cloud.OfAsync <| CloudRef<'T>.Parse(path, fs.FileStore, serializer)
    }

    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    static member Read(cloudRef : CloudRef<'T>) : Cloud<'T> = Cloud.OfAsync <| cloudRef.GetValue()