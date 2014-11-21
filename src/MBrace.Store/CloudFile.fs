namespace Nessos.MBrace.Store

open System
open System.IO
open System.Runtime.Serialization

open Nessos.MBrace

/// Defines a cloud file storage service abstraction
[<AutoSerializable(false)>]
type ICloudFileProvider =
    inherit IResource

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


/// Represents a file stored in the cloud storage service.
[<Sealed; AutoSerializable(true)>]
type CloudFile =
    
    val private providerId : string
    val private path : string

    [<NonSerialized>]
    val mutable private provider : ICloudFileProvider

    [<OnDeserializedAttribute>]
    member private __.OnDeserialized(_ : StreamingContext) =
        __.provider <- ResourceRegistry<ICloudFileProvider>.Resolve __.providerId

    internal new (provider : ICloudFileProvider, path : string) =
        {
            provider = provider
            providerId = ResourceRegistry<ICloudFileProvider>.GetId provider
            path = path
        }

    /// Full path to cloud file.
    member __.Path = __.path
    /// Path of containing folder
    member __.Container = __.provider.GetFileContainer __.path
    /// File name
    member __.Name = __.provider.GetFileName __.path
    /// Cloud service unique identifier
    member __.ProviderId = __.providerId

    /// Asynchronously returns a reading stream to file.
    member __.AsyncRead () = __.provider.BeginRead __.path

    interface ICloudDisposable with
        member __.Dispose () = __.provider.DeleteFile __.path


[<AutoOpen>]
module CloudFileUtils =

    type ICloudFileProvider with
        
        /// <summary>
        ///     Creates a new CloudFile instance using provided serializing function.
        /// </summary>
        /// <param name="path">Path to Cloudfile.</param>
        /// <param name="serializer">Serializing function.</param>
        member p.CreateFile(path : string, serializer : Stream -> Async<unit>) = async {
            use! stream = p.BeginWrite path
            do! serializer stream
            return new CloudFile(p, path)
        }

        /// <summary>
        ///     Creates a CloudFile instance from existing path.
        /// </summary>
        /// <param name="path">Path to be wrapped.</param>
        member p.GetFile(path : string) = async {
            let! exists = p.FileExists path
            return
                if exists then new CloudFile(p, path)
                else
                    raise <| new FileNotFoundException(path)
        }

        /// <summary>
        ///     Enumerates all entries as Cloud file instances.s
        /// </summary>
        /// <param name="container">Cotnainer to be enumerated.</param>
        member p.EnumerateCloudFiles(container : string) = async {
            let! files = p.EnumerateFiles container
            return files |> Array.map (fun f -> new CloudFile(p, f))
        }

        /// <summary>
        ///     Reads the contents of provided cloud file using provided deserializer.
        /// </summary>
        /// <param name="file">cloud file to be read.</param>
        /// <param name="deserializer">deserializing function.</param>
        member p.Read(file : CloudFile, deserializer : Stream -> Async<'T>) = async {
            use! stream = p.BeginRead file.Path
            return! deserializer stream
        }

        /// <summary>
        ///     Copy the contents of stream to given cloud file.
        /// </summary>
        /// <param name="source">Source stream.</param>
        /// <param name="targetFile">Target file.</param>
        member p.OfStream(source : Stream, targetFile : string) = async {
            do! p.OfStream(source, targetFile)
            return new CloudFile(p, targetFile)
        }

        /// <summary>
        ///     Copy the contents of given cloud file to local stream.
        /// </summary>
        /// <param name="sourceFile">Source cloud file.</param>
        /// <param name="target">Target stream.</param>
        member p.ToStream(sourceFile : CloudFile, target : Stream) = p.ToStream(sourceFile.Path, target)

        /// <summary>
        ///     Delete given cloud file.
        /// </summary>
        /// <param name="file">Cloud file.</param>
        member p.Delete(file : CloudFile) = p.DeleteFile file.Path

        /// <summary>
        ///     Checks if cloud file exists in store.
        /// </summary>
        /// <param name="file">File to be examined.</param>
        member p.Exists(file : CloudFile) = p.FileExists file.Path