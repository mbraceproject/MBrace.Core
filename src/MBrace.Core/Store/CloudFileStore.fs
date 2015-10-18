namespace MBrace.Core.Internals

open System
open System.IO

open MBrace.Core

type ETag = string

/// Cloud file storage abstraction
type ICloudFileStore =

    /// Implementation name
    abstract Name : string

    /// Store identifier
    abstract Id : string

    //
    //  Region : Path operations
    //

    /// Indicates whether the file system uses case sensitive paths
    abstract IsCaseSensitiveFileSystem : bool

    /// Gets the root directory for cloud store instance.
    abstract RootDirectory : string

    /// Gets the default directory used by the current cluster.
    abstract DefaultDirectory : string

    /// Creates a copy of the file store implementation with updated default directory.
    abstract WithDefaultDirectory : directory:string -> ICloudFileStore

    /// Generates a random, uniquely specified path to directory
    abstract GetRandomDirectoryName : unit -> string

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

    /// <summary>
    ///     Returns true iff path is absolute under the store uri format.
    /// </summary>
    /// <param name="path">Path to be checked.</param>
    abstract IsPathRooted : path:string -> bool

    //
    //  Region : File/Directory operations
    //

    /// <summary>
    ///     Asynchronously gets the file size in bytes.
    /// </summary>
    /// <param name="path">Path to file.</param>
    abstract GetFileSize : path:string -> Async<int64>

    /// <summary>
    ///     Asynchronously gets the last modification time for given path.
    /// </summary>
    /// <param name="path">Path to file or directory.</param>
    /// <param name="isDirectory">Indicates whether path is intended as directory. Otherwise it will be looked up as file.</param>
    abstract GetLastModifiedTime : path:string * isDirectory:bool -> Async<DateTimeOffset>

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
    ///     Creates a new file in store. If successful returns a writer stream.
    /// </summary>
    /// <param name="path">Path to new file.</param>
    abstract BeginWrite : path:string -> Async<Stream>

    /// <summary>
    ///     Reads from an existing file in store. If successful returns a reader stream.
    /// </summary>
    /// <param name="path">Path to existing file.</param>
    abstract BeginRead : path:string -> Async<Stream>

    /// <summary>
    ///     Creates a new file from provided stream.
    /// </summary>
    /// <param name="targetFile">Target file.</param>
    /// <param name="source">Source stream.</param>
    abstract CopyOfStream : source:Stream * target:string -> Async<unit>

    /// <summary>
    ///     Reads an existing file to target stream.
    /// </summary>
    /// <param name="sourceFile">Source file.</param>
    /// <param name="target">Target stream.</param>
    abstract CopyToStream : sourceFile:string * target:Stream -> Async<unit>

    //
    //  Entity tag API
    //

    /// <summary>
    ///     Asynchronously returns the ETag for provided file, if it exists.
    /// </summary>
    /// <param name="path">Path to file.</param>
    abstract TryGetETag : path:string -> Async<ETag option>

    /// <summary>
    ///     Creates a new file in store. If successful returns a writer stream.
    /// </summary>
    /// <param name="path">Path to new file.</param>
    /// <param name="writer">Asynchronous writer function.</param>
    /// <returns>Returns the write result and the etag of written file.</returns>
    abstract WriteETag : path:string * writer:(Stream -> Async<'R>) -> Async<ETag * 'R>

    /// <summary>
    ///     Attempts to begin reading file from given path,
    ///     provided that supplied etag matches payload.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="etag">ETag to be matched.</param>
    /// <returns>Some reader stream if etag matches, or None if it doesn't.</returns>
    abstract ReadETag : path:string * etag:ETag -> Async<Stream option>

[<AutoOpen>]
module CloudFileStoreUtils =
    
    type ICloudFileStore with

        /// <summary>
        ///     Reads file in store with provided deserializer function.
        /// </summary>
        /// <param name="deserializer">Deserializer function.</param>
        /// <param name="path">Path to file.</param>
        member store.Read<'T>(deserializer : Stream -> Async<'T>, path : string) = async {
            use! stream = store.BeginRead path
            return! deserializer stream
        }

        /// <summary>
        ///     Generates a random path in provided directory.
        /// </summary>
        /// <param name="directory">Container directory.</param>
        member store.GetRandomFilePath (directory : string) =
            let fileName = Path.GetRandomFileName()
            store.Combine [| directory ; fileName |]

        /// Enumerate all directories inside root folder.
        member store.EnumerateRootDirectories () = async {
            let dir = store.RootDirectory
            return! store.EnumerateDirectories(dir)
        }

        /// Creates a copy of the store instance with a unique default directory
        member store.WithUniqueDefaultDirectory () =
            let directory = store.GetRandomDirectoryName()
            store.WithDefaultDirectory directory

        /// <summary>
        ///     Gets the absoluted path of provided path string.
        /// </summary>
        /// <param name="path">Input path string</param>
        member store.GetFullPath(path : string) =
            if store.IsPathRooted path then path
            else
                store.Combine [|store.DefaultDirectory ; path |]

        /// Combines two strings into a single path.
        member store.Combine(path1 : string, path2 : string) = store.Combine [| path1 ; path2 |]
        /// Combines two strings into a single path.
        member store.Combine(path1 : string, path2 : string, path3 : string) = store.Combine [| path1 ; path2 ; path3 |]


namespace MBrace.Core

open System
open System.Runtime.Serialization
open System.Text
open System.Threading.Tasks
open System.IO
open System.IO.Compression

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals

#nowarn "444"

/// Serializable reference to a directory in the cloud store
/// that can be used for accessing contained subdirectories and files.
[<DataContract; Sealed; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudDirectoryInfo =

    [<DataMember(Name = "Store")>]
    val mutable private store : ICloudFileStore

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud directory. This will not create a directory in the local store.
    /// </summary>
    /// <param name="store">Serializable CloudFileStore implementation.</param>
    /// <param name="path">Path to directory.</param>
    new (store : ICloudFileStore, path : string) = { store = store ; path = path }

    /// Gets a unique store identifier
    member d.StoreId = d.store.Id
    
    /// Gets path to directory
    member d.Path = d.path

    /// Gets the last modified time of directory
    member d.LastModifiedTime =
        d.store.GetLastModifiedTime(d.path, isDirectory = true) |> Async.RunSync

    interface ICloudDisposable with
        member d.Dispose () = async {
            return! d.store.DeleteDirectory(d.path, recursiveDelete = true)
        }

    override __.ToString() = __.path
    member private r.StructuredFormatDisplay = r.ToString()

/// Serializable reference to a file in the cloud store
/// that can be used for accessing contained subdirectories and files.
[<DataContract; Sealed; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudFileInfo =

    [<DataMember(Name = "Store")>]
    val mutable private store : ICloudFileStore

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud file. This will not create a file in the local store.
    /// </summary>
    /// <param name="store">Serializable CloudFileStore implementation.</param>
    /// <param name="path">Path to file.</param>
    new (store : ICloudFileStore, path : string) = { store = store ; path = path }
    
    /// Path to cloud file
    member f.Path = f.path

    /// Gets the size (in bytes) of current file if it exists.
    member f.Size : int64 =
        f.store.GetFileSize f.path |> Async.RunSync

    /// Gets the last modified time of current file if it exists.
    member f.LastModifed : DateTimeOffset =
        f.store.GetLastModifiedTime (f.path, isDirectory = false) |> Async.RunSync

    interface ICloudDisposable with
        member f.Dispose () = async { return! f.store.DeleteFile f.path }

    override __.ToString() = __.path
    member private r.StructuredFormatDisplay = r.ToString()

/// Contains static methods used for performing path
/// operations in the cloud store.
type CloudPath =

    /// Gets whether the current cloud file store is case sensitive.
    static member IsCaseSensitive : CloudLocal<bool> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.IsCaseSensitiveFileSystem
    }

    /// Gets the default directory in use by the runtime.
    static member DefaultDirectory : CloudLocal<string> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.DefaultDirectory
    }

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetDirectoryName(path : string) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.GetDirectoryName path
    }

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetFileName(path : string) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.GetFileName path
    }

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    static member Combine(path1 : string, path2 : string) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.Combine [| path1 ; path2 |]
    }

    /// <summary>
    ///     Combines three strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    /// <param name="path3">Third path.</param>
    static member Combine(path1 : string, path2 : string, path3 : string) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.Combine [| path1 ; path2 ; path3 |]
    }

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    static member Combine(paths : string []) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.Combine paths
    }

    /// <summary>
    ///     Gets the absolute path for supplied path string.
    /// </summary>
    /// <param name="path">Input path string.</param>
    static member GetFullPath(path : string) = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.GetFullPath path
    }

    /// Generates a random, uniquely specified path to directory
    static member GetRandomDirectoryName() = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return store.GetRandomDirectoryName()
    }

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    static member GetRandomFileName(?container : string) : CloudLocal<string> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let container = match container with Some c -> c | None -> store.DefaultDirectory
        return store.GetRandomFilePath(container)
    }

/// Contains static methods used for performing
/// directory operations in the cloud store.
type CloudDirectory =

    /// <summary>
    ///     Checks if directory exists in given path.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    static member Exists(dirPath : string) : CloudLocal<bool> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.DirectoryExists dirPath
    }

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to newly created directory.</param>
    static member Create(dirPath : string) : CloudLocal<CloudDirectoryInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        do! Cloud.OfAsync <| store.CreateDirectory(dirPath)
        return new CloudDirectoryInfo(store, dirPath)
    }

    /// <summary>
    ///     Creates a CloudDirectoryInfo instance using given path.
    /// </summary>
    /// <param name="dirPath">Path to cloud directory.</param>
    /// <param name="verify">Verify that file exists before returning. Defaults to true.</param>
    static member GetInfo(dirPath : string, ?verify:bool) : CloudLocal<CloudDirectoryInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        if defaultArg verify true then
            let! exists = Cloud.OfAsync <| store.DirectoryExists dirPath
            if not exists then return raise <| new DirectoryNotFoundException(dirPath)

        return new CloudDirectoryInfo(store, dirPath)
    }

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    static member Delete(dirPath : string, ?recursiveDelete : bool) : CloudLocal<unit> = local {
        let recursiveDelete = defaultArg recursiveDelete false
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.DeleteDirectory(dirPath, recursiveDelete = recursiveDelete)
    }

    /// <summary>
    ///     Gets the latest modified time for given directory.
    /// </summary>
    /// <param name="dirPath">Directory path to be modified.</param>
    static member GetLastModifiedTime(dirPath : string) : CloudLocal<DateTimeOffset> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.GetLastModifiedTime(dirPath, isDirectory = true)
    }

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated.</param>
    static member Enumerate(dirPath : string) : CloudLocal<CloudDirectoryInfo []> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! dirs = Cloud.OfAsync <| store.EnumerateDirectories(dirPath)
        return dirs |> Array.map (fun d -> new CloudDirectoryInfo(store, d))
    }

/// Contains static methods used for performing
/// directory operations in the cloud store.
type CloudFile =

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    static member GetSize(path : string) : CloudLocal<int64> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.GetFileSize path
    }

    /// <summary>
    ///     Gets the last modification time for given file.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    static member GetLastModifiedTime(path : string) : CloudLocal<DateTimeOffset> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.GetLastModifiedTime(path, isDirectory = false)
    }

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    static member Exists(path : string) : CloudLocal<bool> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.FileExists path
    }

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    static member Delete(path : string) : CloudLocal<unit> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.DeleteFile path
    }

    /// <summary>
    ///     Creates a CloudFileInfo instance using given path.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="verify">Verify that file exists before returning. Defaults to true.</param>
    static member GetInfo(path : string, ?verify:bool) : CloudLocal<CloudFileInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        if defaultArg verify true then
            let! exists = Cloud.OfAsync <| store.FileExists path
            if not exists then return raise <| new FileNotFoundException(path)

        return new CloudFileInfo(store, path)
    }

    /// <summary>
    ///     Creates a new file in store and returns a local writer stream.
    /// </summary>
    /// <param name="path">Path to new cloud file.</param>
    static member BeginWrite(path : string) : CloudLocal<System.IO.Stream> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.BeginWrite path
    }

    /// <summary>
    ///     Asynchronously returns a reader function for given path in cloud store, if it exists.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    static member BeginRead<'T>(path : string) : CloudLocal<System.IO.Stream> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        return! Cloud.OfAsync <| store.BeginRead path
    }

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    static member Enumerate(dirPath : string) : CloudLocal<CloudFileInfo []> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let! paths = Cloud.OfAsync <| store.EnumerateFiles(dirPath)
        return paths |> Array.map (fun path -> new CloudFileInfo(store, path))
    }

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to new cloud file.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    static member WriteAllLines(path : string, lines : seq<string>, ?encoding : Encoding) : CloudLocal<CloudFileInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore>()
        use! stream = Cloud.OfAsync <| store.BeginWrite path
        use sw = 
            match encoding with
            | None -> new StreamWriter(stream)
            | Some e -> new StreamWriter(stream, e)

        do for line in lines do sw.WriteLine(line)
        return new CloudFileInfo(store, path)
    }

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="path">Path to Path to cloud file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadLines(path : string, ?encoding : Encoding) : CloudLocal<seq<string>> = local {
        let! store = Cloud.GetResource<ICloudFileStore> ()
        let store = store
        let mkEnumerator () =
            let stream = store.BeginRead path |> Async.RunSync
            let seq = TextReaders.ReadLines(stream, ?encoding = encoding)
            seq.GetEnumerator()

        return Seq.fromEnumerator mkEnumerator
    }

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to Path to cloud file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllLines(path : string, ?encoding : Encoding) : CloudLocal<string []> = local {
        use! stream = CloudFile.BeginRead path
        let lines = TextReaders.ReadLines(stream, ?encoding = encoding)
        return Seq.toArray lines
    }

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    static member WriteAllText(path : string, text : string, ?encoding : Encoding) : CloudLocal<CloudFileInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore>()
        use! stream = Cloud.OfAsync <| store.BeginWrite path
        use sw = 
            match encoding with
            | None -> new StreamWriter(stream)
            | Some e -> new StreamWriter(stream, e)

        do! sw.WriteAsync text |> Async.AwaitTaskCorrect |> Cloud.OfAsync
        return new CloudFileInfo(store, path)
    }

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to Path to cloud file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllText(path : string, ?encoding : Encoding) = local {
        use! stream = CloudFile.BeginRead path
        use sr = 
            match encoding with
            | None -> new StreamReader(stream)
            | Some e -> new StreamReader(stream, e)

        return sr.ReadToEnd()
    }

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="buffer">Source buffer.</param>
    static member WriteAllBytes(path : string, buffer : byte []) : CloudLocal<CloudFileInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore>()
        use! stream = Cloud.OfAsync <| store.BeginWrite path
        do! Cloud.OfAsync <| stream.AsyncWrite(buffer, 0, buffer.Length)
        return new CloudFileInfo(store, path)
    }

    /// <summary>
    ///     Write the contents of a stream directly to a CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to read from. Assumes that the stream is already at the correct position for reading.</param>
    static member WriteStream(path : string, stream : Stream) : CloudLocal<CloudFileInfo> = local {
        let! store = Cloud.GetResource<ICloudFileStore>()
        do! store.CopyOfStream(stream, path) |> Cloud.OfAsync
        return new CloudFileInfo(store, path)
    }

    /// <summary>
    ///     Write the contents of a CloudFile directly to a Stream.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to write to.</param>
    static member ReadStream(path : string, stream : Stream) : CloudLocal<unit> = local {
        let! store = Cloud.GetResource<ICloudFileStore>()
        return! store.CopyToStream(path, stream) |> Cloud.OfAsync
    }

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Path to Path to cloud file.</param>
    static member ReadAllBytes(path : string) : CloudLocal<byte []> = local {
        use! stream = CloudFile.BeginRead path
        use ms = new MemoryStream()
        do! stream.CopyToAsync ms |> Async.AwaitTaskCorrect |> Cloud.OfAsync
        return ms.ToArray()
    }

    /// <summary>
    ///     Uploads a file from local disk to store.
    /// </summary>
    /// <param name="sourcePath">Path to file in local disk.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    static member Upload(sourcePath : string, targetPath : string, ?overwrite : bool, ?compress : bool) : CloudLocal<CloudFileInfo> = local {
        let overwrite = defaultArg overwrite false
        let compress = defaultArg compress false
        let! store = Cloud.GetResource<ICloudFileStore>()
        if not overwrite then
            let! exists = Cloud.OfAsync <| store.FileExists targetPath
            if exists then raise <| new IOException(sprintf "The file '%s' already exists." targetPath)

        use fs = File.OpenRead (Path.GetFullPath sourcePath)

        if compress then
            use! stream = Cloud.OfAsync <| store.BeginWrite targetPath
            use gz = new GZipStream(stream, CompressionLevel.Optimal)
            do! fs.CopyToAsync gz |> Async.AwaitTaskCorrect |> Cloud.OfAsync
        else
            do! Cloud.OfAsync <| store.CopyOfStream(fs, targetPath)

        return new CloudFileInfo(store, targetPath)
    }

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    static member Upload(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?compress : bool) : CloudLocal<CloudFileInfo []> = local {
        let sourcePaths = Seq.toArray sourcePaths
        match sourcePaths |> Array.tryFind (not << File.Exists) with
        | Some notFound -> raise <| new FileNotFoundException(notFound)
        | None -> ()

        let uploadFile (localFile : string) = local {
            let fileName = Path.GetFileName localFile
            let! targetPath = CloudPath.Combine(targetDirectory, fileName)
            return! CloudFile.Upload(localFile, targetPath, ?overwrite = overwrite, ?compress = compress)
        }

        return!
            sourcePaths
            |> Seq.map uploadFile
            |> Local.Parallel
    }

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Source path to file in store.</param>
    /// <param name="targetPath">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    static member Download(sourcePath : string, targetPath : string, ?overwrite : bool, ?decompress : bool) : CloudLocal<unit> = local {
        let overwrite = defaultArg overwrite false
        let decompress = defaultArg decompress false
        let targetPath = Path.GetFullPath targetPath
        let! store = Cloud.GetResource<ICloudFileStore> ()
        if not overwrite && File.Exists targetPath then
            raise <| new IOException(sprintf "The file '%s' already exists." targetPath)

        use stream =
            let fs = File.OpenWrite targetPath
            if decompress then
                new GZipStream(fs, CompressionMode.Decompress) :> Stream
            else
                fs :> _

        do! Cloud.OfAsync <| store.CopyToStream(sourcePath, stream)
    }

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    static member Download(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?decompress : bool) : CloudLocal<string []> = local {
        let download (path : string) = local {
            let localFile = Path.Combine(targetDirectory, Path.GetFileName path)
            do! CloudFile.Download(path, localFile, ?overwrite = overwrite, ?decompress = decompress)
            return localFile
        }

        return!
            sourcePaths
            |> Seq.map download
            |> Local.Parallel
    }