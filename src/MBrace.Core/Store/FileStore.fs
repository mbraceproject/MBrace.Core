namespace Nessos.MBrace.Store

open System
open System.IO

/// Defines a cloud file storage abstraction
type ICloudFileStore =

    /// unique cloud file store identifier
    abstract UUID : string

    /// Returns a serializable file store factory for the current instance.
    abstract GetFactory : unit -> ICloudFileStoreFactory

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
    ///     Returns the file size in bytes.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetFileSize : path:string -> Async<int64>

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
    ///     Creates a new container in store.
    /// </summary>
    /// <param name="container">Container id.</param>
    abstract CreateContainer : container:string -> Async<unit>
        
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

/// Defines a serializable abstract factory for a file store instance.
/// Used for pushing filestore definitions across machines
and ICloudFileStoreFactory =
    abstract Create : unit -> ICloudFileStore