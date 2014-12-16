namespace Nessos.MBrace.Store

open System
open System.IO

/// Cloud storage entity identifier
type ICloudStorageEntity =
    /// Type identifier for entity
    abstract Type : string
    /// Entity unique identifier
    abstract Id : string

/// Defines a cloud file storage abstraction
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
    abstract CreateUniqueDirectoryPath : unit -> string

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

/// Store configuration passed to the continuation execution context
type CloudFileStoreConfiguration = 
    {
        /// File store.
        FileStore : ICloudFileStore
        /// Default directory used by current execution context.
        DefaultDirectory : string
    }

[<AutoOpen>]
module CloudFileStoreUtils =
    
    type ICloudFileStore with
        // TODO : retry policy?
        /// <summary>
        ///     Creates a new file in store with provided serializer function.
        /// </summary>
        /// <param name="serializer">Serializer function.</param>
        /// <param name="path">Path to file. Defaults to auto-generated path.</param>
        member cfs.Create(serializer : Stream -> Async<unit>, path : string) = async {
            use! stream = cfs.BeginWrite path
            do! serializer stream
        }

        /// <summary>
        ///     Reads file in store with provided deserializer function.
        /// </summary>
        /// <param name="deserializer">Deserializer function.</param>
        /// <param name="path">Path to file.</param>
        member cfs.ReadAsync<'T>(deserializer : Stream -> Async<'T>, path : string) = async {
            use! stream = cfs.BeginWrite path
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

// Combinators for MBrace

namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Continuation
open Nessos.MBrace.Store

#nowarn "444"

/// Collection of file store operations
/// for cloud workflows
type FileStore =

    /// Returns the file store instance carried in current execution context.
    static member GetFileStore () = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore
    }

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetDirectoryName(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.GetDirectoryName path
    }

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetFileName(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.GetFileName path
    }

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    static member Combine(path1 : string, path2 : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.Combine [| path1 ; path2 |]
    }

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    static member Combine(paths : string []) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.Combine paths
    }

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    static member Combine(directory : string, fileNames : seq<string>) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fileNames |> Seq.map (fun f -> fs.FileStore.Combine [|directory ; f |]) |> Seq.toArray
    }

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to file.</param>
    static member GetFileSize(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.GetFileSize path
    }

    /// Generates a random, uniquely specified path to directory
    static member CreateUniqueDirectoryPath() = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.CreateUniqueDirectoryPath()
    }

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to file.</param>
    static member FileExists(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.FileExists path
    }

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the root directory.</param>
    static member EnumerateFiles(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.GetRootDirectory()

        return! Cloud.OfAsync <| fs.FileStore.EnumerateFiles(directory)
    }

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">File path.</param>
    static member DeleteFile(directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DeleteFile directory
    }

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    static member DirectoryExists(directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DirectoryExists directory
    }

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    static member CreateDirectory(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.CreateUniqueDirectoryPath()

        return! Cloud.OfAsync <| fs.FileStore.CreateDirectory(directory)
    }

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    static member DeleteDirectory(directory : string, ?recursiveDelete : bool) = cloud {
        let recursiveDelete = defaultArg recursiveDelete false
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DeleteDirectory(directory, recursiveDelete = recursiveDelete)
    }

    /// <summary>
    ///     Enumerates all directories in directory.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    static member EnumerateDirectories(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.GetRootDirectory()

        return! Cloud.OfAsync <| fs.FileStore.EnumerateDirectories(directory)
    }

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    static member CreateFile(serializer : Stream -> Async<unit>, ?path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = match path with Some p -> p | None -> fs.FileStore.GetRandomFilePath fs.DefaultDirectory
        do! Cloud.OfAsync <| fs.FileStore.Create(serializer, path)
        return path
    }

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    static member CreateFile(serializer : Stream -> Async<unit>, directory : string, fileName : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = fs.FileStore.Combine [|directory ; fileName|]
        do! Cloud.OfAsync <| fs.FileStore.Create(serializer, path)
        return path
    }

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="path">Path to file.</param>
    static member ReadFile<'T>(deserializer : Stream -> Async<'T>, path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.ReadAsync(deserializer, path)
    }