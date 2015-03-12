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
type CloudCell<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "UUID")>]
    val mutable private uuid : string
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer option

    internal new (path, serializer) =
        let uuid = Guid.NewGuid().ToString()
        { path = path ; uuid = uuid ; serializer = serializer }

    member private r.GetValueFromStore(config : CloudFileStoreConfiguration) = async {
        let serializer = match r.serializer with Some s -> s | None -> config.Serializer
        use! stream = config.FileStore.BeginRead r.path
        // deserialize payload
        return serializer.Deserialize<'T>(stream, leaveOpen = false)
    }

    /// Path to cloud cell payload in store
    member r.Path = r.path

    /// Dereference the cloud cell
    member r.Value = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
        match config.Cache |> Option.bind (fun c -> c.TryFind r.uuid) with
        | Some v -> return v :?> 'T
        | None -> return! ofAsync <| r.GetValueFromStore(config)
    }

    /// Caches the cloud cell value to the local execution contexts. Returns true iff successful.
    member r.PopulateCache() = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
        match config.Cache with
        | None -> return false
        | Some c ->
            if c.ContainsKey r.uuid then return true
            else
                let! v = ofAsync <| r.GetValueFromStore(config)
                return c.Add(r.uuid, v)
    }

    /// Indicates if array is cached in local execution context
    member c.IsCachedLocally = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        return config.Cache |> Option.exists(fun ch -> ch.ContainsKey c.uuid)
    }

    /// Gets the size of cloud cell in bytes
    member r.Size = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
        return! ofAsync <| config.FileStore.GetFileSize r.path
    }

    override r.ToString() = sprintf "CloudCell[%O] at %s" typeof<'T> r.path
    member private r.StructuredFormatDisplay = r.ToString()

    interface ICloudDisposable with
        member r.Dispose () = local {
            let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
            return! ofAsync <| config.FileStore.DeleteFile r.path
        }

    interface ICloudStorageEntity with
        member r.Type = sprintf "cloudref:%O" typeof<'T>
        member r.Id = r.path

#nowarn "444"

type CloudCell =
    
    /// <summary>
    ///     Creates a new local reference to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud cell value.</param>
    /// <param name="directory">FileStore directory used for cloud cell. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    static member New(value : 'T, ?directory : string, ?serializer : ISerializer) = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
        let directory = defaultArg directory config.DefaultDirectory
        let _serializer = match serializer with Some s -> s | None -> config.Serializer
        let path = config.FileStore.GetRandomFilePath directory
        let writer (stream : Stream) = async {
            // write value
            _serializer.Serialize(stream, value, leaveOpen = false)
        }
        do! ofAsync <| config.FileStore.Write(path, writer)
        return new CloudCell<'T>(path, serializer)
    }

    /// <summary>
    ///     Parses a cloud cell of given type with provided serializer. If successful, returns the cloud cell instance.
    /// </summary>
    /// <param name="path">Path to cloud cell.</param>
    /// <param name="serializer">Serializer for cloud cell.</param>
    static member Parse<'T>(path : string, ?serializer : ISerializer, ?force : bool) = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration>()
        let _serializer = match serializer with Some s -> s | None -> config.Serializer
        let cell = new CloudCell<'T>(path, serializer)
        if defaultArg force false then
            let! _ = cell.PopulateCache() in ()
        else
            let! exists = ofAsync <| config.FileStore.FileExists path
            if not exists then return raise <| new FileNotFoundException(path)
        return cell
    }

    /// <summary>
    ///     Creates a CloudCell from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromFile<'T>(path : string, deserializer : Stream -> 'T, ?force : bool) : Local<CloudCell<'T>> = local {
        let serializer =
            {
                new ISerializer with
                    member __.Id = "Deserializer Lambda"
                    member __.Serialize(_,_,_) = raise <| new NotSupportedException()
                    member __.Deserialize(_,_) = raise <| new NotSupportedException()
                    member __.SeqSerialize(_,_,_) = raise <| new NotSupportedException()
                    member __.SeqDeserialize<'a>(source,_) = deserializer source :> obj :?> seq<'a>
            }

        return! CloudCell.Parse(path, serializer, ?force = force)
    }

    /// <summary>
    ///     Creates a CloudCell parsing a text file.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromTextFile<'T>(path : string, textDeserializer : StreamReader -> 'T, ?encoding : Encoding, ?force : bool) : Local<CloudCell<'T>> = local {
        let deserializer (stream : Stream) =
            let sr =
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 

        return! CloudCell.FromFile(path, deserializer, ?force = force)
    }

    /// <summary>
    ///     Dereference a Cloud cell.
    /// </summary>
    /// <param name="cloudCell">CloudCell to be dereferenced.</param>
    static member Read(cloudCell : CloudCell<'T>) : Local<'T> = cloudCell.Value

    /// <summary>
    ///     Cache a cloud cell to local execution context.
    /// </summary>
    /// <param name="cloudCell">Cloud cell input.</param>
    static member Cache(cloudCell : CloudCell<'T>) : Local<bool> = local { return! cloudCell.PopulateCache() }

    /// <summary>
    ///     Disposes cloud cell from store.
    /// </summary>
    /// <param name="cloudCell">Cloud cell to be deleted.</param>
    static member Dispose(cloudCell : CloudCell<'T>) : Local<unit> = local { return! (cloudCell :> ICloudDisposable).Dispose() }