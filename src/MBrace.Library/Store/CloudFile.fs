namespace Nessos.MBrace

open System
open System.Text
open System.Runtime.Serialization
open System.IO

open Nessos.MBrace.Continuation
open Nessos.MBrace.Store

#nowarn "444"

/// Represents a file reference bound to specific cloud store instance
[<Sealed ; AutoSerializable(true) ; DataContract>]
type CloudFile private (fileStore : ICloudFileStore, path : string) =

    [<DataMember(Name = "Path")>]
    let path = path

    [<DataMember(Name = "StoreId")>]
    let storeId = fileStore.GetFileStoreDescriptor()

    [<IgnoreDataMember>]
    let mutable fileStore = Some fileStore

    // delayed store bootstrapping after deserialization
    let getStore() =
        match fileStore with
        | Some fs -> fs
        | None ->
            let fs = storeId.Recover()
            fileStore <- Some fs
            fs

    /// Full path to cloud file.
    member __.Path = path
    /// Path of containing folder
    member __.DirectoryName = getStore().GetDirectoryName path
    /// File name
    member __.FileName = getStore().GetFileName path
    /// Cloud service unique identifier
    member __.StoreId = storeId.Id

    // Note : async members must delay getStore() to avoid
    // fileStore being captured in closures

    /// Returns the file size in bytes
    member __.GetSizeAsync () = async { return! getStore().GetFileSize path }

    /// Asynchronously returns a reading stream to file.
    member __.BeginRead () : Async<Stream> = async { return! getStore().BeginRead path }

    /// <summary>
    ///     Copy file contents to local stream.
    /// </summary>
    /// <param name="target">Target stream.</param>
    member __.CopyToStream (target : Stream) = async { return! getStore().ToStream(path, target) }

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
        member __.Dispose () = async { return! getStore().DeleteFile path }

    static member Create(path : string, fileStore : ICloudFileStore) =
        new CloudFile(fileStore, path)


    /// <summary> 
    ///     Create a new file in the storage with the specified folder and name.
    ///     Use the serialize function to write to the underlying stream.
    /// </summary>
    /// <param name="serializer">Function that will write data on the underlying stream.</param>
    /// <param name="path">Target uri for given cloud file. Defaults to runtime-assigned path.</param>
    static member New(serializer : Stream -> Async<unit>, ?path : string) : Cloud<CloudFile> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! path = FileStore.CreateFile(serializer, ?path = path)
        return new CloudFile(config.FileStore, path)
    }

    /// <summary> 
    ///     Create a new file in the storage with the specified folder and name.
    ///     Use the serialize function to write to the underlying stream.
    /// </summary>
    /// <param name="serializer">Function that will write data on the underlying stream.</param>
    /// <param name="path">Target uri for given cloud file. Defaults to runtime-assigned path.</param>
    static member New(serializer : Stream -> Async<unit>, directory, fileName) : Cloud<CloudFile> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! path = FileStore.CreateFile(serializer, directory, fileName)
        return new CloudFile(config.FileStore, path)
    }

    /// <summary>
    ///     Returns an existing cloud file instance from provided path.
    /// </summary>
    /// <param name="path">Input path to cloud file.</param>
    static member FromPath(path : string) : Cloud<CloudFile> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return new CloudFile(config.FileStore, path)
    }

    /// <summary> 
    ///     Read the contents of a CloudFile using the given deserialize/reader function.
    /// </summary>
    /// <param name="cloudFile">CloudFile to read.</param>
    /// <param name="deserializer">Function that reads data from the underlying stream.</param>
    static member Read(cloudFile : CloudFile, deserializer : Stream -> Async<'T>) : Cloud<'T> =
        Cloud.OfAsync <| cloudFile.Read deserializer

    /// <summary> 
    ///     Returns all files in given directory as CloudFiles.
    /// </summary>
    /// <param name="directory">Directory to enumerate. Defaults to execution context.</param>
    static member Enumerate(?directory : string) : Cloud<CloudFile []> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory = match directory with Some d -> d | None -> config.DefaultDirectory
        let! files = Cloud.OfAsync <| config.FileStore.EnumerateFiles directory
        return files |> Array.map (fun f -> new CloudFile(config.FileStore, f))
    }


    /// <summary>
    ///     Reads a CloudFile as a sequence of lines.
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadLines(file : CloudFile, ?encoding : Encoding) = cloud {
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
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    static member WriteLines(lines : seq<string>, ?encoding : Encoding, ?path : string) = cloud {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)
            for line in lines do
                do! sw.WriteLineAsync(line)
        }

        return! CloudFile.New(writer, ?path = path)
    }

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    /// <param name="encoding">Text encoding.</param>
    static member ReadAllText(file : CloudFile, ?encoding : Encoding) = cloud {
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
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllText(text : string, ?encoding : Encoding, ?path : string) = cloud {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)
            do! sw.WriteLineAsync text
        }
        return! CloudFile.New(writer, ?path = path)
    }
        
    /// <summary>
    ///     Dump the contents of given CloudFile as byte[].
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    static member ReadAllBytes(file : CloudFile) = cloud {
        let reader (stream : Stream) = async {
            use ms = new MemoryStream()
            do! stream.CopyToAsync ms
            return ms.ToArray()
        }

        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    static member WriteAllBytes(buffer : byte [], ?path : string) = cloud {
        let writer (stream : Stream) = stream.AsyncWrite(buffer, 0, buffer.Length)
        return! CloudFile.New(writer, ?path = path)
    }