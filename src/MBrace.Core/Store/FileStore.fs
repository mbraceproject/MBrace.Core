namespace Nessos.MBrace.Store

open System
open System.IO

/// Defines a cloud file storage abstraction
type ICloudFileStore =

    /// Implementation name
    abstract Name : string

    /// Store identifier
    abstract Id : string

    /// Returns a serializable file store descriptor
    /// that can be used in remote processes.
    abstract GetFileStoreDescriptor : unit -> ICloudFileStoreDescriptor

    //
    //  Region : Path operations
    //

    /// Returns the root directory for cloud store instance.
    abstract GetRootDirectory : unit -> string

    /// Generates a random, uniquely specified path to directory
    abstract GetUniqueDirectoryPath : unit -> string

    /// <summary>
    ///     Returns a normal form for path. Returns None if invalid format.
    /// </summary>
    /// <param name="path">Input filepath.</param>
    abstract TryGetFullPath : path:string -> string option

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input filepath.</param>
    abstract GetDirectoryName : path:string -> string

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    abstract GetFileName : path:string -> string

    /// <summary>
    ///     Combines an array of strings into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    abstract Combine : paths:string [] -> string

    //
    //  Region : File/Directory operations
    //

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
    ///     Gets all files that exist in given container
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    abstract EnumerateFiles : directory:string -> Async<string []>

    /// <summary>
    ///     Deletes file in given path
    /// </summary>
    /// <param name="path">File path.</param>
    abstract DeleteFile : path:string -> Async<unit>

    /// <summary>
    ///     Checks if container exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    abstract DirectoryExists : directory:string -> Async<bool>

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory</param>
    abstract CreateDirectory : directory:string -> Async<string>
        
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

/// Defines a serializable file store descriptor
/// used for recovering instances in remote processes.
and ICloudFileStoreDescriptor =
    /// Implementation name
    abstract Name : string
    /// Descriptor Identifier
    abstract Id : string
    /// Recovers the file store instance locally
    abstract Recover : unit -> ICloudFileStore

/// Store configuration record passed to the continuation execution context
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
        member cfs.Create(serializer : Stream -> Async<unit>, path : string) = async {
            use! stream = cfs.BeginWrite path
            do! serializer stream
        }

        member cfs.ReadAsync<'T>(deserializer : Stream -> Async<'T>, path : string) = async {
            use! stream = cfs.BeginWrite path
            return! deserializer stream
        }

        member cfs.GetRandomFilePath (directory : string) =
            let fileName = Path.GetRandomFileName()
            cfs.Combine [| directory ; fileName |]

// Combinators for MBrace

namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Continuation
open Nessos.MBrace.Store

#nowarn "444"

type FileStore =

    static member GetFileStore () = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore
    }

    static member GetDirectoryName(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.GetDirectoryName path
    }

    static member GetFileName(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.GetFileName path
    }

    static member Combine(path1 : string, path2 : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.Combine [| path1 ; path2 |]
    }

    static member Combine(paths : string []) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return fs.FileStore.Combine paths
    }

    static member GetFileSize(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.GetFileSize path
    }

    static member FileExists(path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.FileExists path
    }

    static member EnumerateFiles(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.GetRootDirectory()

        return! Cloud.OfAsync <| fs.FileStore.EnumerateFiles(directory)
    }

    static member DeleteFile(directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DeleteFile directory
    }

    static member DirectoryExists(directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DirectoryExists directory
    }

    static member CreateDirectory(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.GetUniqueDirectoryPath()

        return! Cloud.OfAsync <| fs.FileStore.CreateDirectory(directory)
    }

    static member DeleteDirectory(directory : string, ?recursiveDelete : bool) = cloud {
        let recursiveDelete = defaultArg recursiveDelete false
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.DeleteDirectory(directory, recursiveDelete = recursiveDelete)
    }

    static member EnumerateDirectories(?directory : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> fs.FileStore.GetRootDirectory()

        return! Cloud.OfAsync <| fs.FileStore.EnumerateDirectories(directory)
    }

    static member CreateFile(serializer : Stream -> Async<unit>, ?path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = match path with Some p -> p | None -> fs.FileStore.GetRandomFilePath fs.DefaultDirectory
        do! Cloud.OfAsync <| fs.FileStore.Create(serializer, path)
        return path
    }

    static member CreateFile(serializer : Stream -> Async<unit>, directory : string, fileName : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = fs.FileStore.Combine [|directory ; fileName|]
        do! Cloud.OfAsync <| fs.FileStore.Create(serializer, path)
        return path
    }

    static member ReadFile<'T>(deserializer : Stream -> Async<'T>, path : string) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| fs.FileStore.ReadAsync(deserializer, path)
    }