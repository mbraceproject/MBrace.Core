﻿namespace MBrace.Store

open System
open System.IO

open MBrace.Core

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
    inherit ICloudDisposable
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
    static member Create(fileStore : ICloudFileStore, serializer : ISerializer, ?defaultDirectory : string) =
        {
            FileStore = fileStore
            DefaultDirectory = match defaultDirectory with Some d -> d | None -> fileStore.GetRandomDirectoryName()
            Serializer = serializer
        }

[<AutoOpen>]
module CloudFileStoreUtils =
    
    type ICloudFileStore with

        /// <summary>
        ///     Reads file in store with provided deserializer function.
        /// </summary>
        /// <param name="deserializer">Deserializer function.</param>
        /// <param name="path">Path to file.</param>
        member cfs.Read<'T>(deserializer : Stream -> Async<'T>, path : string) = async {
            use! stream = cfs.BeginRead path
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


namespace MBrace.Core

open System
open System.Runtime.Serialization
open System.Text
open System.Threading.Tasks
open System.IO

open MBrace.Continuation
open MBrace.Store

#nowarn "444"

/// Generic FileStore utilities
type FileStore =

    /// Returns the file store instance carried in current execution context.
    static member Current = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore
    }

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetDirectoryName(path : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetDirectoryName path
    }

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetFileName(path : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetFileName path
    }

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    static member Combine(path1 : string, path2 : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine [| path1 ; path2 |]
    }

    /// <summary>
    ///     Combines three strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    /// <param name="path3">Third path.</param>
    static member Combine(path1 : string, path2 : string, path3 : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine [| path1 ; path2 ; path3 |]
    }

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    static member Combine(paths : string []) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine paths
    }

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    static member Combine(directory : string, fileNames : seq<string>) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine(directory, fileNames)
    }

    /// Generates a random, uniquely specified path to directory
    static member GetRandomDirectoryName() = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetRandomDirectoryName()
    }

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    static member GetRandomFileName(?container : string) : Local<string> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let container = match container with Some c -> c | None -> config.DefaultDirectory
        return config.FileStore.GetRandomFilePath(container)
    }

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    static member GetRandomFileName(?container : CloudDirectory) : Local<string> =
        let container : string option = container |> Option.map (fun d -> d.Path)
        FileStore.GetRandomFileName(?container = container)

/// Represents a directory found in the cloud store
and [<DataContract; Sealed; StructuredFormatDisplay("{StructuredFormatDisplay}")>] CloudDirectory =

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud directory. This will not create a directory in the local store.
    /// </summary>
    /// <param name="path">Path to directory.</param>
    new (path : string) = { path = path }
    
    /// Path to directory
    member d.Path = d.path

    interface ICloudDisposable with
        member d.Dispose () = CloudDirectory.Delete(d.Path, recursiveDelete = true)

    override __.ToString() = __.path
    member private r.StructuredFormatDisplay = r.ToString()

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    static member Exists(dirPath : string) : Local<bool> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DirectoryExists dirPath
    }

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to randomly generated directory.</param>
    static member Create(?dirPath : string) : Local<CloudDirectory> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let dirPath =
            match dirPath with
            | Some p -> p
            | None -> config.FileStore.GetRandomDirectoryName()

        do! ofAsync <| config.FileStore.CreateDirectory(dirPath)
        return new CloudDirectory(dirPath)
    }

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    static member Delete(dirPath : string, ?recursiveDelete : bool) : Local<unit> = local {
        let recursiveDelete = defaultArg recursiveDelete false
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DeleteDirectory(dirPath, recursiveDelete = recursiveDelete)
    }

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    static member Enumerate(?dirPath : string) : Local<CloudDirectory []> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let dirPath =
            match dirPath with
            | Some p -> p
            | None -> config.FileStore.GetRootDirectory()

        let! dirs = ofAsync <| config.FileStore.EnumerateDirectories(dirPath)
        return dirs |> Array.map (fun d -> new CloudDirectory(d))
    }

/// Represents a file found in the local store
and [<DataContract; Sealed; StructuredFormatDisplay("{StructuredFormatDisplay}")>] CloudFile =

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud file. This will not create a file in the local store.
    /// </summary>
    /// <param name="path">Path to file.</param>
    new (path : string) = { path = path }
    
    /// Path to cloud file
    member f.Path = f.path

    /// Gets the size (in bytes) of current file if it exists.
    member f.Size = CloudFile.GetSize f.path

    interface ICloudDisposable with
        member f.Dispose () = CloudFile.Delete f.Path

    override __.ToString() = __.path
    member private r.StructuredFormatDisplay = r.ToString()

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member GetSize(path : string) : Local<int64> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.GetFileSize path
    }

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member Exists(path : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.FileExists path
    }

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member Delete(path : string) = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DeleteFile path
    }

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    static member Create(serializer : Stream -> Async<unit>, ?path : string) : Local<CloudFile> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = match path with Some p -> p | None -> config.FileStore.GetRandomFilePath config.DefaultDirectory
        do! ofAsync <| config.FileStore.Write(path, serializer)
        return new CloudFile(path)
    }

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="dirPath">Path to containing directory.</param>
    /// <param name="fileName">File name.</param>
    static member Create(serializer : Stream -> Async<unit>, dirPath : string, fileName : string) : Local<CloudFile> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = config.FileStore.Combine [|dirPath ; fileName|]
        do! ofAsync <| config.FileStore.Write(path, serializer)
        return new CloudFile(path)
    }

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="leaveOpen">Do not dispose stream after deserialization. Defaults to false.</param>
    static member Read<'T>(path : string, deserializer : Stream -> Async<'T>, ?leaveOpen : bool) : Local<'T> = local {
        let leaveOpen = defaultArg leaveOpen false
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| async {
            if leaveOpen then
                let! stream = config.FileStore.BeginRead(path)
                return! deserializer stream
            else
                return! config.FileStore.Read(deserializer, path)
        }
    }

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to the process directory.</param>
    static member Enumerate(?dirPath : string) : Local<CloudFile []> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let dirPath =
            match dirPath with
            | Some d -> d
            | None -> config.DefaultDirectory

        let! paths = ofAsync <| config.FileStore.EnumerateFiles(dirPath)
        return paths |> Array.map (fun path -> new CloudFile(path))
    }

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to cloud file.</param>
    static member WriteAllLines(lines : seq<string>, ?encoding : Encoding, ?path : string) : Local<CloudFile> = local {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)

            do for line in lines do
                do sw.WriteLine(line)
        }

        return! CloudFile.Create(writer, ?path = path)
    }

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadLines(path : string, ?encoding : Encoding) : Local<seq<string>> = local {
        let reader (stream : Stream) = async {
            return seq { 
                use sr = 
                    match encoding with
                    | None -> new StreamReader(stream)
                    | Some e -> new StreamReader(stream, e)
                while not sr.EndOfStream do
                    yield sr.ReadLine()
            }
        }

        return! CloudFile.Read(path, reader, leaveOpen = true)
    }

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllLines(path : string, ?encoding : Encoding) : Local<string []> = local {
        let reader (stream : Stream) = async {
            let ra = new ResizeArray<string> ()
            use sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            do while not sr.EndOfStream do
                ra.Add <| sr.ReadLine()

            return ra.ToArray()
        }

        return! CloudFile.Read(path, reader)
    }

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllText(text : string, ?path : string, ?encoding : Encoding) : Local<CloudFile> = local {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)
            do! sw.WriteLineAsync text
        }

        return! CloudFile.Create(writer, ?path = path)
    }

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllText(path : string, ?encoding : Encoding) = local {
        let reader (stream : Stream) = async {
            use sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)
            return sr.ReadToEnd()
        }
        return! CloudFile.Read(path, reader)
    }

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllBytes(buffer : byte [], ?path : string) : Local<CloudFile> = local {
        let writer (stream : Stream) = stream.AsyncWrite(buffer, 0, buffer.Length)
        return! CloudFile.Create(writer, ?path = path)
    }
        
    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    static member ReadAllBytes(path : string) : Local<byte []> = local {
        let reader (stream : Stream) = async {
            use ms = new MemoryStream()
            do! stream.CopyToAsync ms
            return ms.ToArray()
        }

        return! CloudFile.Read(path, reader)
    }

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="localFile">Local path to file.</param>
    /// <param name="targetDirectory">Containing directory in cloud store. Defaults to process default.</param>
    static member Upload(localFile : string, ?targetDirectory : string) : Local<CloudFile> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let targetDirectory = defaultArg targetDirectory config.DefaultDirectory
        use fs = File.OpenRead localFile
        let fileName = Path.GetFileName localFile
        let targetPath = config.FileStore.Combine(targetDirectory, fileName)
        do! ofAsync <| config.FileStore.OfStream(fs, targetPath)
        return new CloudFile(targetPath)
    }