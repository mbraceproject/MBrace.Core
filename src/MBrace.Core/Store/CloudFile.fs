namespace MBrace

open System
open System.Runtime.Serialization
open System.Text
open System.Threading.Tasks
open System.IO

open MBrace.Continuation
open MBrace.Store

#nowarn "444"

[<AutoOpen>]
module private CloudFileUtils =

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)


/// Generic FileStore utilities
type FileStore =

    /// Returns the file store instance carried in current execution context.
    static member Current = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore
    }

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetDirectoryName(path : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetDirectoryName path
    }

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    static member GetFileName(path : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetFileName path
    }

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    static member Combine(path1 : string, path2 : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine [| path1 ; path2 |]
    }

    /// <summary>
    ///     Combines three strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    /// <param name="path3">Third path.</param>
    static member Combine(path1 : string, path2 : string, path3 : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine [| path1 ; path2 ; path3 |]
    }

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    static member Combine(paths : string []) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine paths
    }

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    static member Combine(directory : string, fileNames : seq<string>) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.Combine(directory, fileNames)
    }

    /// Generates a random, uniquely specified path to directory
    static member GetRandomDirectoryName() = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return config.FileStore.GetRandomDirectoryName()
    }

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    static member GetRandomFileName(?container : string) : Cloud<string> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let container = match container with Some c -> c | None -> config.DefaultDirectory
        return config.FileStore.GetRandomFilePath(container)
    }

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    static member GetRandomFileName(?container : CloudDirectory) : Cloud<string> =
        let container : string option = container |> Option.map (fun d -> d.Path)
        FileStore.GetRandomFileName(?container = container)

/// Represents a directory found in the cloud store
and [<DataContract; Sealed>] CloudDirectory =

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud directory. This will not create a directory in the cloud store.
    /// </summary>
    /// <param name="path">Path to directory.</param>
    new (path : string) = { path = path }
    
    /// Path to directory
    member d.Path = d.path

    interface ICloudDisposable with
        member d.Dispose () = CloudDirectory.Delete(d, recursiveDelete = true)

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    static member Exists(directory : string) : Cloud<bool> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DirectoryExists directory
    }

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    static member Exists(directory : CloudDirectory) =
        CloudDirectory.Exists directory.Path

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    static member Create(?directory : string) : Cloud<CloudDirectory> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> config.FileStore.GetRandomDirectoryName()

        do! ofAsync <| config.FileStore.CreateDirectory(directory)
        return new CloudDirectory(directory)
    }

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    static member Delete(directory : string, ?recursiveDelete : bool) : Cloud<unit> = cloud {
        let recursiveDelete = defaultArg recursiveDelete false
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DeleteDirectory(directory, recursiveDelete = recursiveDelete)
    }

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    static member Delete(directory : CloudDirectory, ?recursiveDelete : bool) =
        CloudDirectory.Delete(directory.Path, ?recursiveDelete = recursiveDelete)

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    static member Enumerate(?directory : string) : Cloud<CloudDirectory []> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> config.FileStore.GetRootDirectory()

        let! dirs = ofAsync <| config.FileStore.EnumerateDirectories(directory)
        return dirs |> Array.map (fun d -> new CloudDirectory(d))
    }

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    static member Enumerate(?directory : CloudDirectory) : Cloud<CloudDirectory []> =
        CloudDirectory.Enumerate(?directory = (directory |> Option.map (fun d -> d.Path)))

/// Represents a file found in the cloud store
and [<DataContract; Sealed>] CloudFile =

    [<DataMember(Name = "Path")>]
    val mutable private path : string

    /// <summary>
    ///     Defines a reference to a cloud file. This will not create a file in the cloud store.
    /// </summary>
    /// <param name="path">Path to file.</param>
    new (path : string) = { path = path }
    
    /// Path to cloud file
    member f.Path = f.path

    interface ICloudDisposable with
        member f.Dispose () = CloudFile.Delete f

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member GetSize(path : string) : Cloud<int64> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.GetFileSize path
    }

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="file">Input file.</param>
    static member GetSize(file : CloudFile) =
        CloudFile.GetSize(file.Path)

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member Exists(path : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.FileExists path
    }

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="file">Input file.</param>
    static member Exists(file : CloudFile) =
        CloudFile.Exists(file.Path)

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member Delete(path : string) = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.DeleteFile path
    }

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="file">Input file.</param>
    static member Delete(file : CloudFile) =
        CloudFile.Delete(file.Path)

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    static member Create(serializer : Stream -> Async<unit>, ?path : string) : Cloud<CloudFile> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = match path with Some p -> p | None -> config.FileStore.GetRandomFilePath config.DefaultDirectory
        do! ofAsync <| config.FileStore.Write(path, serializer)
        return new CloudFile(path)
    }

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    static member Create(serializer : Stream -> Async<unit>, directory : string, fileName : string) : Cloud<CloudFile> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let path = config.FileStore.Combine [|directory ; fileName|]
        do! ofAsync <| config.FileStore.Write(path, serializer)
        return new CloudFile(path)
    }

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="leaveOpen">Do not dispose stream after deserialization. Defaults to false.</param>
    static member Read<'T>(path : string, deserializer : Stream -> Async<'T>, ?leaveOpen : bool) : Cloud<'T> = cloud {
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
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="leaveOpen">Do not dispose stream after deserialization. Defaults to false.</param>
    static member Read<'T>(file : CloudFile, deserializer : Stream -> Async<'T>, ?leaveOpen : bool) : Cloud<'T> = 
        CloudFile.Read(file.Path, deserializer, ?leaveOpen = leaveOpen)

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    static member Enumerate(?directory : string) : Cloud<CloudFile []> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory =
            match directory with
            | Some d -> d
            | None -> config.DefaultDirectory

        let! paths = ofAsync <| config.FileStore.EnumerateFiles(directory)
        return paths |> Array.map (fun path -> new CloudFile(path))
    }

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    static member Enumerate(?directory : CloudDirectory) : Cloud<CloudFile []> =
        CloudFile.Enumerate(?directory = (directory |> Option.map (fun d -> d.Path)))

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    static member WriteAllLines(lines : seq<string>, ?encoding : Encoding, ?path : string) : Cloud<CloudFile> = cloud {
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
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadLines(file : string, ?encoding : Encoding) : Cloud<seq<string>> = cloud {
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

        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadLines(file : CloudFile, ?encoding : Encoding) = 
        CloudFile.ReadLines(file.Path, ?encoding = encoding)


    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllLines(file : string, ?encoding : Encoding) : Cloud<string []> = cloud {
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

        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllLines(file : CloudFile, ?encoding : Encoding) = 
        CloudFile.ReadAllLines(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllText(text : string, ?path : string, ?encoding : Encoding) : Cloud<CloudFile> = cloud {
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
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllText(file : string, ?encoding : Encoding) = cloud {
        let reader (stream : Stream) = async {
            use sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)
            return sr.ReadToEnd()
        }
        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllText(file : CloudFile, ?encoding : Encoding) =
        CloudFile.ReadAllText(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllBytes(buffer : byte [], ?path : string) : Cloud<CloudFile> = cloud {
        let writer (stream : Stream) = stream.AsyncWrite(buffer, 0, buffer.Length)
        return! CloudFile.Create(writer, ?path = path)
    }
        
    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Input file.</param>
    static member ReadAllBytes(path : string) : Cloud<byte []> = cloud {
        let reader (stream : Stream) = async {
            use ms = new MemoryStream()
            do! stream.CopyToAsync ms
            return ms.ToArray()
        }

        return! CloudFile.Read(path, reader)
    }

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="file">Input file.</param>
    static member ReadAllBytes(file : CloudFile) : Cloud<byte []> =
        CloudFile.ReadAllBytes(file.Path)