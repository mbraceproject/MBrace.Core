namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references are cached locally for performance.
[<Sealed; AutoSerializable(true)>]
type CloudRef<'T> private (value : 'T, file : string, fileStore : ICloudFileStore, serializer : ISerializer) =
    
    let storeId = fileStore.GetFileStoreDescriptor()
    let serializerId = serializer.GetSerializerDescriptor()

    // conveniently, the uninitialized value for optional fields coincides with 'None'.
    // a more correct approach would initialize using an OnDeserialized callback
    [<NonSerialized>]
    let mutable cachedValue = Some value

    /// Asynchronously dereferences the cloud ref.
    member __.GetValue () = async {
        match cachedValue with
        | Some v -> return v
        | None ->
            let store = storeId.Recover()
            let serializer = serializerId.Recover()
            use! stream = store.BeginRead file
            let v = serializer.Deserialize<'T>(stream, leaveOpen = false)
            cachedValue <- Some v
            return v
    }

    /// Synchronously dereferences the cloud ref.
    member __.Value =
        match cachedValue with
        | Some v -> v
        | None -> __.GetValue() |> Async.RunSync

    interface ICloudDisposable with
        member __.Dispose () = async { return! storeId.Recover().DeleteFile file }

    static member CreateAsync(value : 'T, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let path = fileStore.GetRandomFilePath directory
        do! async { use! stream = fileStore.BeginWrite path in do serializer.Serialize(stream, value, leaveOpen = false) }
        return new CloudRef<'T>(value, path, fileStore, serializer)
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

        return! Cloud.OfAsync <| CloudRef<'T>.CreateAsync(value, directory, fs.FileStore, serializer)
    }

    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    static member Read(cloudRef : CloudRef<'T>) : Cloud<'T> = Cloud.OfAsync <| cloudRef.GetValue()