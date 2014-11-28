namespace Nessos.MBrace.Store

open System
open System.IO

/// Defines a cloud file storage abstraction
type ICloudFileStore =

    /// unique cloud file store identifier
    abstract UUID : string

    /// <summary>
    ///     Returns the container for given file path.
    /// </summary>
    /// <param name="path">Input filepath.</param>
    abstract GetFileContainer : path:string -> string

    /// <summary>
    ///     Returns the file name for given file path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetFileName : path:string -> string

    /// <summary>
    ///     Returns the file size in bytes.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetFileSize : path:string -> Async<int64>

    /// <summary>
    ///     Checks if path to filer/container is of valid format.
    /// </summary>
    /// <param name="path"></param>
    abstract IsValidPath : path:string -> bool

    /// Creates a unique path for container name.
    abstract CreateUniqueContainerName : unit -> string

    /// <summary>
    ///     Creates a unique file name inside provided container.
    /// </summary>
    /// <param name="container">container path.</param>
    abstract CreateUniqueFileName : container:string -> string

    /// <summary>
    ///     Checks if file exists in given path
    /// </summary>
    /// <param name="path">File path.</param>
    abstract FileExists : path:string -> Async<bool>

    /// <summary>
    ///     Deletes file in given path
    /// </summary>
    /// <param name="path">File path.</param>
    abstract DeleteFile : path:string -> Async<unit>

    /// <summary>
    ///     Gets all files that exist in given container
    /// </summary>
    /// <param name="path">Path to file container.</param>
    abstract EnumerateFiles : container:string -> Async<string []>

    /// <summary>
    ///     Checks if container exists in given path
    /// </summary>
    /// <param name="container">file container.</param>
    abstract ContainerExists : container:string -> Async<bool>
        
    /// <summary>
    ///     Deletes container in given path
    /// </summary>
    /// <param name="container">file container.</param>
    abstract DeleteContainer : container:string -> Async<unit>

    /// Get all containers that exist in storage service
    abstract EnumerateContainers : unit -> Async<string []>

    /// <summary>
    ///     Creates a new file in store. If successful returns a writing stream.
    /// </summary>
    /// <param name="path">Path to new file.</param>
    abstract BeginWrite : path:string -> Async<Stream>

    /// <summary>
    ///     Reads from an existing file in store. If successful returns a reading stream.
    /// </summary>
    /// <param name="path">Path to existing file.</param>
    abstract BeginRead : path:string -> Async<Stream>

    /// <summary>
    ///     Creates a new file from provided stream.
    /// </summary>
    /// <param name="targetFile">Target file.</param>
    /// <param name="source">Source stream.</param>
    abstract OfStream : source:Stream * target:string -> Async<unit>

    /// <summary>
    ///     Reads an existing file to target stream.
    /// </summary>
    /// <param name="sourceFile">Source file.</param>
    /// <param name="target">Target stream.</param>
    abstract ToStream : sourceFile:string * target:Stream -> Async<unit>




///// Store configuration container
//type ICloudStore =
//    /// Universal unique identifier for store service/configuration
//    abstract UUID : string
//
//    /// Cloud file store
//    abstract FileStore : ICloudFileStore
//
//    /// Cloud table store
//    abstract TableStore : ICloudTableStore option
//
//    /// Returns a serializable value for activation of store
//    /// session in remote machines
//    abstract GetActivator : unit -> ICloudStoreActivator
//
///// Cloud store session activation factory
//and ICloudStoreActivator =
//    /// Creates a new CloudStore instance
//    abstract Create : unit -> ICloudStore
//
///// Global store registy; used for bootstrapping store connection settings on
///// data primitive deserialization.
//type CloudStoreRegistry private () =
//    static let registry = new System.Collections.Concurrent.ConcurrentDictionary<string, ICloudStore> ()
//
//    /// <summary>
//    ///     Registers a cloudstore instance. 
//    /// </summary>
//    /// <param name="store">Store to be registered.</param>
//    /// <param name="force">Force overwrite. Defaults to false.</param>
//    static member Register(store : ICloudStore, ?force : bool) : unit = 
//        if defaultArg force false then
//            registry.AddOrUpdate(store.UUID, store, fun _ _ -> store) |> ignore
//        elif registry.TryAdd(store.UUID, store) then ()
//        else
//            let msg = sprintf "CloudStoreRegistry: a store with id '%O' already exists in registry." id
//            invalidOp msg
//
//    /// <summary>
//    ///     Resolves a registerd cloudstore instance by UUID.
//    /// </summary>
//    /// <param name="id">CloudStore UUID.</param>
//    static member Resolve(id : string) : ICloudStore = 
//        let mutable store = Unchecked.defaultof<ICloudStore>
//        if registry.TryGetValue(id, &store) then store
//        else
//            let msg = sprintf "CloudStoreRegistry: no store with id '%O' could be resolved." id
//            invalidOp msg
//
///// Provides a collection of parameters required
///// for cloud storage operations in MBrace.
//type CloudStoreConfiguration =
//    {
//        /// Cloud store implementation
//        Store : ICloudStore
//        /// Serializer used for persisting .NET objects in store
//        Serializer : ISerializer
//        /// Default FileStore container
//        DefaultContainer : string
//    }