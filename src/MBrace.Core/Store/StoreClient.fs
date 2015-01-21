namespace MBrace

#nowarn "0444"

open MBrace
open MBrace.Store
open MBrace.Continuation
open System.IO
open System.Text

[<Sealed>]
/// Atom methods for MBrace.
type CloudAtomClient internal (registry : ResourceRegistry) =
    do registry.Resolve<CloudAtomConfiguration>()
       |> ignore
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    
    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member __.New<'T>(initial : 'T) : Async<ICloudAtom<'T>> =  MBrace.CloudAtom.New(initial) |> toAsync
       
    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member __.Read(atom : ICloudAtom<'T>) : Async<'T> = MBrace.CloudAtom.Read(atom) |> toAsync

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member __.Update (updateF : 'T -> 'T) (atom : ICloudAtom<'T>) : Async<unit> = 
        atom.Update updateF |> toAsync

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member __.Force (value : 'T) (atom : ICloudAtom<'T>) : Async<unit> = 
        atom.Force value |> toAsync

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="trasactF"></param>
    /// <param name="atom"></param>
    member __.Transact (trasactF : 'T -> 'R * 'T) (atom : ICloudAtom<'T>) : Async<'R> = 
        atom.Transact trasactF |> toAsync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member __.Delete (atom : ICloudAtom<'T>) = Cloud.Dispose atom |> toAsync

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    member __.IsSupportedValue(value : 'T) = MBrace.CloudAtom.IsSupportedValue value |> toAsync

[<Sealed>]
/// Channel methods for MBrace.
type CloudChannelClient internal (registry : ResourceRegistry) =
    do registry.Resolve<CloudChannelConfiguration>()
       |> ignore
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)

    /// Creates a new channel instance.
    member __.New<'T>() = MBrace.CloudChannel.New<'T>() |> toAsync

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="channel">Target channel.</param>
    member __.Send<'T> (message : 'T) (channel : ISendPort<'T>) = MBrace.CloudChannel.Send<'T> message channel |> toAsync

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    member __.Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) =  MBrace.CloudChannel.Receive(channel, ?timeout = timeout) |> toAsync

[<Sealed>]
/// Collection of file store operations
type CloudPathClient internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    
    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetDirectoryName(path : string) = MBrace.FileStore.GetDirectoryName(path) |> toAsync

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetFileName(path : string) = MBrace.FileStore.GetFileName(path) |> toAsync

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    member __.Combine(path1 : string, path2 : string) = MBrace.FileStore.Combine(path1, path2) |> toAsync

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    member __.Combine(paths : string []) = MBrace.FileStore.Combine(paths) |> toAsync

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    member __.Combine(directory : string, fileNames : seq<string>) = MBrace.FileStore.Combine(directory, fileNames) |> toAsync

    /// <summary>
    /// Returns current ICloudFileStore instance.
    /// </summary>
//    member __.Current : Async<ICloudFileStore> = FileStore.Current |> toAsync
                   
    /// Generates a random, uniquely specified path to directory
    member __.GetRandomDirectoryName() = MBrace.FileStore.GetRandomDirectoryName() |> toAsync

[<Sealed>]
/// Collection of file store operations
type CloudDirectoryClient internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    
    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member __.Exists(directory : string) : Async<bool> = 
        CloudDirectory.Exists(directory) |> toAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member __.Exists(directory : CloudDirectory) : Async<bool> =
        __.Exists directory.Path 

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    member __.Create(?directory : string) : Async<CloudDirectory> =
        CloudDirectory.Create(?directory = directory) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member __.Delete(directory : string, ?recursiveDelete : bool) : Async<unit> = 
        CloudDirectory.Delete(directory, ?recursiveDelete = recursiveDelete) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member __.Delete(directory : CloudDirectory, ?recursiveDelete : bool) =
        __.Delete(directory.Path, ?recursiveDelete = recursiveDelete)

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    member __.Enumerate(?directory : string) : Async<CloudDirectory []> = 
        CloudDirectory.Enumerate(?directory = directory) |> toAsync

[<Sealed>]
type CloudFileClient internal (registry : ResourceRegistry) =
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.GetSize(path : string) : Async<int64> = 
        CloudFile.GetSize(path) |> toAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.GetSize(file : CloudFile) =
        __.GetSize(file.Path)

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.Exists(path : string) = 
        CloudFile.Exists(path) |> toAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.Exists(file : CloudFile) =
        __.Exists(file.Path)

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.Delete(path : string) = 
        CloudFile.Delete(path) |> toAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.Delete(file : CloudFile) =
        __.Delete(file.Path)

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    member __.Create(serializer : Stream -> Async<unit>, ?path : string) : Async<CloudFile> = 
        CloudFile.Create(serializer, ?path = path) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    member __.Create(serializer : Stream -> Async<unit>, directory : string, fileName : string) : Async<CloudFile> = 
        CloudFile.Create(serializer, directory, fileName) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member __.Read<'T>(path : string, deserializer : Stream -> Async<'T>) : Async<'T> = 
        CloudFile.Read<'T>(path, deserializer) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member __.Read<'T>(file : CloudFile, deserializer : Stream -> Async<'T>) : Async<'T> = 
        __.Read(file.Path, deserializer)

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    member __.Enumerate(?directory : string) : Async<CloudFile []> = 
        CloudFile.Enumerate(?directory = directory) |> toAsync

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    member __.WriteLines(lines : seq<string>, ?encoding : Encoding, ?path : string) : Async<CloudFile> = 
        CloudFile.WriteLines(lines, ?encoding = encoding, ?path = path) |> toAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadLines(file : string, ?encoding : Encoding) : Async<string []> =
        CloudFile.ReadLines(file, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadLines(file : CloudFile, ?encoding : Encoding) = 
        __.ReadLines(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllText(text : string, ?encoding : Encoding, ?path : string) : Async<CloudFile> = 
        CloudFile.WriteAllText(text, ?encoding = encoding, ?path = path) |> toAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllText(file : string, ?encoding : Encoding) =
        CloudFile.ReadAllText(file, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllText(file : CloudFile, ?encoding : Encoding) =
        __.ReadAllText(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllBytes(buffer : byte [], ?path : string) : Async<CloudFile> =
       CloudFile.WriteAllBytes(buffer, ?path = path) |> toAsync
        
    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.ReadAllBytes(path : string) : Async<byte []> =
        CloudFile.ReadAllBytes(path) |> toAsync

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.ReadAllBytes(file : CloudFile) : Async<byte []> =
        __.ReadAllBytes(file.Path)

[<Sealed>]
type FileStoreClient internal (registry : ResourceRegistry) =
    do registry.Resolve<CloudFileStoreConfiguration>()
       |> ignore
    let pathClient = new CloudPathClient(registry)
    let directoryClient = new CloudDirectoryClient(registry)
    let fileClient = new CloudFileClient(registry)

    /// CloudFileStore client.
    member __.File = fileClient
    /// CloudDirectory client.
    member __.Directory = directoryClient
    /// CloudFile client.
    member __.Path = pathClient 

[<Sealed>]
/// Common client operations on CloudAtom, CloudChannel and CloudFile primitives.
type StoreClient internal (registry : ResourceRegistry) =
    let atomClient    = lazy CloudAtomClient(registry)
    let channelClient = lazy CloudChannelClient(registry)
    let fileStore     = lazy FileStoreClient(registry)

    /// CloudAtom client.
    member __.CloudAtom = atomClient.Value
    /// CloudChannel client.
    member __.CloudChannel = channelClient.Value
    /// CloudFileStore client.
    member __.Store = fileStore.Value

    /// Create a new StoreClient instance from given resources.
    /// Resources must contain CloudFileStoreConfiguration, CloudAtomConfiguration and CloudChannelConfiguration values.
    static member CreateFromResources(resources : ResourceRegistry) =
        new StoreClient(resources)