namespace Nessos.MBrace

//open System
//open System.IO
//
//open Nessos.MBrace.Store
//
///// Represents a file reference bound to specific cloud store instance
//[<Sealed; AutoSerializable(true)>]
//type CloudFile internal (fileStore : ICloudFileStore, path : string) =
//
//    let storeFactory = fileStore.GetFactory()
//
//    [<NonSerialized>]
//    let mutable fileStore = Some fileStore
//
//    // delayed store bootstrapping after deserialization
//    let getStore() =
//        match fileStore with
//        | Some fs -> fs
//        | None ->
//            let fs = storeFactory.Create()
//            fileStore <- Some fs
//            fs
//
//    /// Full path to cloud file.
//    member __.Path = path
//    /// Path of containing folder
//    member __.DirectoryName = getStore().GetDirectoryName path
//    /// File name
//    member __.FileName = getStore().GetFileName path
//    /// Cloud service unique identifier
//    member __.StoreId = storeFactory.Id
//
//    // Note : async members must delay getStore() to avoid
//    // fileStore being captured in closures
//
//    /// Returns the file size in bytes
//    member __.GetSizeAsync () = async { return! getStore().GetFileSize path }
//
//    /// Asynchronously returns a reading stream to file.
//    member __.BeginRead () : Async<Stream> = async { return! getStore().BeginRead path }
//
//    /// <summary>
//    ///     Copy file contents to local stream.
//    /// </summary>
//    /// <param name="target">Target stream.</param>
//    member __.CopyToStream (target : Stream) = async { return! getStore().ToStream(path, target) }
//
//    /// <summary>
//    ///     Reads the contents of provided cloud file using provided deserializer.
//    /// </summary>
//    /// <param name="file">cloud file to be read.</param>
//    /// <param name="deserializer">deserializing function.</param>
//    member __.Read(deserializer : Stream -> Async<'T>) : Async<'T> = async {
//        use! stream = __.BeginRead()
//        return! deserializer stream
//    }
//
//    interface ICloudDisposable with
//        member __.Dispose () = async { return! getStore().DeleteFile path }
//
//
//namespace Nessos.MBrace.Store
//
//open System.IO
//open Nessos.MBrace
//
//[<AutoOpen>]
//module CloudFileUtils =
//
//    type ICloudFileStore with
//        
//        /// <summary>
//        ///     Creates a new CloudFile instance using provided serializing function.
//        /// </summary>
//        /// <param name="path">Path to Cloudfile.</param>
//        /// <param name="serializer">Serializing function.</param>
//        member fs.CreateFile(path : string, serializer : Stream -> Async<unit>) = async {
//            use! stream = fs.BeginWrite path
//            do! serializer stream
//            return new CloudFile(fs, path)
//        }
//
//        /// <summary>
//        ///     Creates a new CloudFile instance copying data from local stream.
//        /// </summary>
//        /// <param name="path">Path to CloudFile.</param>
//        /// <param name="source">Source stream.</param>
//        member fs.CreateFile(path : string, source : Stream) = async {
//            do! fs.OfStream(source, path)
//            return new CloudFile(fs, path)
//        }
//
//        /// <summary>
//        ///     Creates a CloudFile instance from existing path.
//        /// </summary>
//        /// <param name="path">Path to be wrapped.</param>
//        member fs.FromPath(path : string) = async {
//            let! exists = fs.FileExists path
//            return
//                if exists then new CloudFile(fs, path)
//                else
//                    raise <| new FileNotFoundException(path)
//        }
//
//        /// <summary>
//        ///     Enumerates all entries as Cloud file instances.
//        /// </summary>
//        /// <param name="container">Cotnainer to be enumerated.</param>
//        member fs.EnumerateCloudFiles(container : string) = async {
//            let! files = fs.EnumerateFiles container
//            return files |> Array.map (fun f -> new CloudFile(fs, f))
//        }
//
//        /// <summary>
//        ///     Checks if cloud file exists in store.
//        /// </summary>
//        /// <param name="file">File to be examined.</param>
//        member fs.Exists(file : CloudFile) = fs.FileExists file.Path