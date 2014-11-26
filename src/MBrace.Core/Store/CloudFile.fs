namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Store

/// Represents a file stored in the cloud storage service.
[<Sealed; AutoSerializable(true)>]
type CloudFile internal (store : ICloudStore, path : string) =

    [<NonSerialized>]
    let mutable fileStore = Some store.FileStore
    let storeId = store.UUID
    // delayed store bootstrapping after deserialization
    let getStore() =
        match fileStore with
        | Some fs -> fs
        | None ->
            let fs = CloudStoreRegistry.Resolve(storeId).FileStore
            fileStore <- Some fs
            fs

    /// Full path to cloud file.
    member __.Path = path
    /// Path of containing folder
    member __.Container = getStore().GetFileContainer path
    /// File name
    member __.Name = getStore().GetFileName path
    /// Cloud service unique identifier
    member __.StoreId = storeId

    /// Returns the file size in bytes
    member __.GetSizeAsync () = getStore().GetFileSize path

    /// Asynchronously returns a reading stream to file.
    member __.BeginRead () : Async<Stream> = getStore().BeginRead path

    /// <summary>
    ///     Reads the contents of provided cloud file using provided deserializer.
    /// </summary>
    /// <param name="file">cloud file to be read.</param>
    /// <param name="deserializer">deserializing function.</param>
    member __.Read(deserializer : Stream -> Async<'T>) : Async<'T> = async {
        use! stream = __.BeginRead()
        return! deserializer stream
    }

    interface ICloudDisposable with
        member __.Dispose () = getStore().DeleteFile path


namespace Nessos.MBrace.Store

open System.IO
open Nessos.MBrace

[<AutoOpen>]
module CloudFileUtils =

    type ICloudStore with
        
        /// <summary>
        ///     Creates a new CloudFile instance using provided serializing function.
        /// </summary>
        /// <param name="path">Path to Cloudfile.</param>
        /// <param name="serializer">Serializing function.</param>
        member s.CreateFile(path : string, serializer : Stream -> Async<unit>) = async {
            use! stream = s.FileStore.BeginWrite path
            do! serializer stream
            return new CloudFile(s, path)
        }

        /// <summary>
        ///     Creates a CloudFile instance from existing path.
        /// </summary>
        /// <param name="path">Path to be wrapped.</param>
        member s.FromPath(path : string) = async {
            let! exists = s.FileStore.FileExists path
            return
                if exists then new CloudFile(s, path)
                else
                    raise <| new FileNotFoundException(path)
        }

        /// <summary>
        ///     Enumerates all entries as Cloud file instances.
        /// </summary>
        /// <param name="container">Cotnainer to be enumerated.</param>
        member s.EnumerateCloudFiles(container : string) = async {
            let! files = s.FileStore.EnumerateFiles container
            return files |> Array.map (fun f -> new CloudFile(s, f))
        }

        /// <summary>
        ///     Delete given cloud file.
        /// </summary>
        /// <param name="file">Cloud file.</param>
        member s.Delete(file : CloudFile) = (file :> ICloudDisposable).Dispose()

        /// <summary>
        ///     Checks if cloud file exists in store.
        /// </summary>
        /// <param name="file">File to be examined.</param>
        member s.Exists(file : CloudFile) = s.FileStore.FileExists file.Path