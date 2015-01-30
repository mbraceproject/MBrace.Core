namespace MBrace.Store

open System
open System.IO

/// Cloud file storage abstraction
type ICloudFileStore =

    /// Implementation name
    abstract Name : string

    /// Store identifier
    abstract Id : string

    //
    //  Region : Path operations
    //

    /// Returns the root directory for cloud store instance.
    abstract GetRootDirectory : unit -> string

    /// Generates a random, uniquely specified path to directory
    abstract GetRandomDirectoryName : unit -> string

    /// <summary>
    ///     Returns a normal form for path. Returns None if invalid format.
    /// </summary>
    /// <param name="path">Input filepath.</param>
    abstract TryGetFullPath : path:string -> string option

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetDirectoryName : path:string -> string

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetFileName : path:string -> string

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    abstract Combine : paths:string [] -> string

    //
    //  Region : File/Directory operations
    //

    /// <summary>
    ///     Returns the file size in bytes.
    /// </summary>
    /// <param name="path">Path to file.</param>
    abstract GetFileSize : path:string -> Async<int64>

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to file.</param>
    abstract FileExists : path:string -> Async<bool>

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    abstract EnumerateFiles : directory:string -> Async<string []>

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">File path.</param>
    abstract DeleteFile : path:string -> Async<unit>

    /// <summary>
    ///     Checks if directory exists in given path.
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    abstract DirectoryExists : directory:string -> Async<bool>

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory</param>
    abstract CreateDirectory : directory:string -> Async<unit>
        
    /// <summary>
    ///     Deletes provided directory.
    /// </summary>
    /// <param name="directory">file container.</param>
    /// <param name="recursive">Delete recursively.</param>
    abstract DeleteDirectory : directory:string * recursiveDelete:bool -> Async<unit>

    /// <summary>
    ///     Get all directories that exist in given directory.
    /// </summary>
    /// <param name="directory">Directory to enumerate.</param>
    abstract EnumerateDirectories : directory:string -> Async<string []>

    //
    //  Region : File read/write API
    //

    /// <summary>
    ///     Creates a new file in store. If successful returns a writing stream.
    /// </summary>
    /// <param name="path">Path to new file.</param>
    /// <param name="writer">Asynchronous writer function.</param>
    abstract Write : path:string * writer:(Stream -> Async<'R>) -> Async<'R>

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

/// Cloud storage entity identifier
type ICloudStorageEntity =
    /// Type identifier for entity
    abstract Type : string
    /// Entity unique identifier
    abstract Id : string

/// Store configuration passed to the continuation execution context
type CloudFileStoreConfiguration = 
    {
        /// File store.
        FileStore : ICloudFileStore
        /// Default directory used by current execution context.
        DefaultDirectory : string
        // Local caching facility
        Cache : IObjectCache option
        // Default serializer
        Serializer : ISerializer
    }
with
    /// <summary>
    ///     Creates a store configuration instance using provided components.
    /// </summary>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    /// <param name="defaultDirectory">Default directory for current process. Defaults to auto generated.</param>
    /// <param name="cache">Object cache. Defaults to no cache.</param>
    static member Create(fileStore : ICloudFileStore, serializer : ISerializer, ?defaultDirectory : string, ?cache : IObjectCache) =
        {
            FileStore = fileStore
            DefaultDirectory = match defaultDirectory with Some d -> d | None -> fileStore.GetRandomDirectoryName()
            Cache = cache
            Serializer = serializer
        }

[<AutoOpen>]
module CloudFileStoreUtils =
    
    type ICloudFileStore with

        /// <summary>
        ///     Reads file in store with provided deserializer function.
        ///     The provided function is responsible for releasing the stream.
        /// </summary>
        /// <param name="deserializer">Deserializer function.</param>
        /// <param name="path">Path to file.</param>
        member cfs.Read<'T>(deserializer : Stream -> Async<'T>, path : string) = async {
            let! stream = cfs.BeginRead path
            return! deserializer stream
        }

        /// <summary>
        ///     Generates a random path in provided directory.
        /// </summary>
        /// <param name="directory">Container directory.</param>
        member cfs.GetRandomFilePath (directory : string) =
            let fileName = Path.GetRandomFileName()
            cfs.Combine [| directory ; fileName |]

        /// Enumerate all directories inside root folder.
        member cfs.EnumerateRootDirectories () = async {
            let dir = cfs.GetRootDirectory()
            return! cfs.EnumerateDirectories(dir)
        }

        /// Combines two strings into a single path.
        member cfs.Combine(path1 : string, path2 : string) = cfs.Combine [| path1 ; path2 |]
        /// Combines two strings into a single path.
        member cfs.Combine(path1 : string, path2 : string, path3 : string) = cfs.Combine [| path1 ; path2 ; path3 |]

        /// <summary>
        ///     Combines a collection of file names with a given path prefix.
        /// </summary>
        /// <param name="container">Path prefix.</param>
        /// <param name="fileNames">File name collections.</param>
        member cfs.Combine(container : string, fileNames : seq<string>) =
            fileNames |> Seq.map (fun f -> cfs.Combine [|container ; f |]) |> Seq.toArray