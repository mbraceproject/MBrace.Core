namespace MBrace.Store

#nowarn "0444"

open System.IO
open System.Text

open MBrace
open MBrace.Store
open MBrace.Continuation

[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudAtom API
type CloudAtomClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let config = registry.Resolve<CloudAtomConfiguration>()

    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member c.CreateAsync<'T>(initial : 'T, ?container : string) : Async<ICloudAtom<'T>> =
        CloudAtom.New(initial, ?container = container) |> toAsync

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member c.Create<'T>(initial : 'T, ?container : string) : ICloudAtom<'T> =
        c.CreateAsync(initial, ?container = container) |> toSync
       
    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member c.ReadAsync(atom : ICloudAtom<'T>) : Async<'T> = 
        CloudAtom.Read(atom) |> toAsync

    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member c.Read(atom : ICloudAtom<'T>) : 'T = 
        c.ReadAsync(atom) |> toSync

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.UpdateAsync (atom : ICloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int): Async<unit> =
        CloudAtom.Update(atom, updater, ?maxRetries = maxRetries) |> toAsync

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.Update (atom : ICloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int): unit = 
        c.UpdateAsync (atom, updater, ?maxRetries = maxRetries) |> toSync

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member c.ForceAsync (atom : ICloudAtom<'T>, value : 'T) : Async<unit> =
        CloudAtom.Force(atom, value) |> toAsync

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member c.Force (atom : ICloudAtom<'T>, value : 'T) : unit = 
        c.ForceAsync(atom, value) |> toSync

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="trasactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.TransactAsync (atom : ICloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : Async<'R> =
        CloudAtom.Transact(atom, transactF, ?maxRetries = maxRetries) |> toAsync

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="trasactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.Transact (atom : ICloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : 'R = 
        c.TransactAsync(atom, transactF, ?maxRetries = maxRetries) |> toSync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.DeleteAsync (atom : ICloudAtom<'T>) : Async<unit> = 
        CloudAtom.Delete atom |> toAsync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.Delete (atom : ICloudAtom<'T>) : unit = 
        c.DeleteAsync atom |> toSync

    /// <summary>
    ///     Deletes the provided atom container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainerAsync (container : string) : Async<unit> = 
        CloudAtom.DeleteContainer container |> toAsync

    /// <summary>
    ///     Deletes the provided atom container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainer (container : string) : unit = 
        c.DeleteContainerAsync container |> toSync

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    member __.IsSupportedValue(value : 'T) : bool = 
        config.AtomProvider.IsSupportedValue value

    /// <summary>
    /// Create a new FileStoreClient instance from given resources.
    /// Resources must contain CloudAtomConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new CloudAtomClient(resources)


[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudAtom API
type CloudChannelClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let config = registry.Resolve<CloudChannelConfiguration>()
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new channel instance.
    /// </summary>
    /// <param name="container">Container for cloud channel.</param>
    member c.CreateAsync<'T>(?container : string) : Async<ISendPort<'T> * IReceivePort<'T>> = 
        CloudChannel.New<'T>(?container = container) |> toAsync

    /// <summary>
    ///     Creates a new channel instance.
    /// </summary>
    /// <param name="container">Container for cloud channel.</param>
    member c.Create<'T>(?container : string) : ISendPort<'T> * IReceivePort<'T> = 
        c.CreateAsync<'T>(?container = container) |> toSync

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="channel">Target channel.</param>
    /// <param name="message">Message to send.</param>
    member c.SendAsync<'T> (channel : ISendPort<'T>, message : 'T) : Async<unit> = 
        CloudChannel.Send<'T> (channel, message) |> toAsync

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="channel">Target channel.</param>
    /// <param name="message">Message to send.</param>
    member c.Send<'T> (channel : ISendPort<'T>, message : 'T) : unit = 
        c.SendAsync<'T>(channel, message) |> toSync

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    member c.ReceiveAsync<'T> (channel : IReceivePort<'T>, ?timeout : int) : Async<'T> = 
        CloudChannel.Receive(channel, ?timeout = timeout) |> toAsync

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    member c.Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) : 'T = 
        c.ReceiveAsync(channel, ?timeout = timeout) |> toSync

    /// <summary>
    ///     Deletes the provided channel instance.
    /// </summary>
    /// <param name="channel">Channel to be deleted.</param>
    member c.DeleteAsync(channel : IReceivePort<'T>) : Async<unit> = 
        CloudChannel.Delete channel |> toAsync

    /// <summary>
    ///     Deletes the provided channel instance.
    /// </summary>
    /// <param name="channel">Channel to be deleted.</param>    
    member c.Delete(channel : IReceivePort<'T>) : unit = c.DeleteAsync channel |> toSync

    /// <summary>
    ///     Deletes the provided channel container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainerAsync (container : string): Async<unit> = 
        CloudChannel.DeleteContainer container |> toAsync

    /// <summary>
    ///     Deletes the provided channel container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainer (container : string) : unit = 
        c.DeleteContainerAsync container |> toSync

    /// <summary>
    /// Create a new FileStoreClient instance from given resources.
    /// Resources must contain CloudChannelConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new CloudChannelClient(resources)


[<Sealed; AutoSerializable(false)>]
/// Collection of path-related file store methods.
type CloudPathClient internal (config : CloudFileStoreConfiguration) =

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetDirectoryName(path : string) = config.FileStore.GetDirectoryName path

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetFileName(path : string) = config.FileStore.GetFileName path

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    member __.Combine(path1 : string, path2 : string) = config.FileStore.Combine [| path1 ; path2 |]

    /// <summary>
    ///     Combines three strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    /// <param name="path3">Third path.</param>
    member __.Combine(path1 : string, path2 : string, path3 : string) = config.FileStore.Combine [| path1 ; path2 ; path3 |]

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    member __.Combine(paths : string []) = config.FileStore.Combine paths

    /// <summary>
    ///     Combines a collection of file names with provided directory prefix.
    /// </summary>
    /// <param name="directory">Directory prefix path.</param>
    /// <param name="fileNames">File names to be combined.</param>
    member __.Combine(directory : string, fileNames : seq<string>) = config.FileStore.Combine(directory, fileNames)
                   
    /// Generates a random, uniquely specified path to directory
    member __.GetRandomDirectoryName() = config.FileStore.GetRandomDirectoryName()

[<Sealed; AutoSerializable(false)>]
/// Collection of file store operations
type CloudDirectoryClient internal (registry : ResourceRegistry) =

    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf
    
    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member c.ExistsAsync(directory : string) : Async<bool> = 
        CloudDirectory.Exists(directory) |> toAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member c.ExistsAsync(directory : CloudDirectory) : Async<bool> =
        c.ExistsAsync directory.Path

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member c.Exists(directory : string) : bool = 
        c.ExistsAsync directory |> toSync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="directory">Path to directory.</param>
    member c.Exists(directory : CloudDirectory) : bool =
        c.Exists directory.Path

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    member c.CreateAsync(?directory : string) : Async<CloudDirectory> =
        CloudDirectory.Create(?directory = directory) |> toAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to randomly generated directory.</param>
    member c.Create(?directory : string) : CloudDirectory =
        c.CreateAsync(?directory = directory) |> toSync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.DeleteAsync(directory : string, ?recursiveDelete : bool) : Async<unit> = 
        CloudDirectory.Delete(directory, ?recursiveDelete = recursiveDelete) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.DeleteAsync(directory : CloudDirectory, ?recursiveDelete : bool) : Async<unit> =
        c.DeleteAsync(directory.Path, ?recursiveDelete = recursiveDelete)

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.Delete(directory : string, ?recursiveDelete : bool) : unit = 
        c.DeleteAsync(directory, ?recursiveDelete = recursiveDelete) |> toSync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="directory">Directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.Delete(directory : CloudDirectory, ?recursiveDelete : bool) : unit = 
        c.DeleteAsync(directory, ?recursiveDelete = recursiveDelete) |> toSync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    member c.EnumerateAsync(?directory : string) : Async<CloudDirectory []> = 
        CloudDirectory.Enumerate(?directory = directory) |> toAsync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="directory">Directory to be enumerated. Defaults to root directory.</param>
    member c.Enumerate(?directory : string) : CloudDirectory [] = 
        c.EnumerateAsync(?directory = directory) |> toSync

[<Sealed; AutoSerializable(false)>]
/// Collection of file store operations
type CloudFileClient internal (registry : ResourceRegistry) =

    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.GetSizeAsync(path : string) : Async<int64> = 
        CloudFile.GetSize(path) |> toAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.GetSizeAsync(file : CloudFile) : Async<int64> =
        CloudFile.GetSize(file.Path) |> toAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.GetSize(path : string) : int64 = 
        c.GetSizeAsync(path) |> toSync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.GetSize(file : CloudFile) : int64 = 
        c.GetSizeAsync(file) |> toSync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.ExistsAsync(path : string) : Async<bool> = 
        CloudFile.Exists(path) |> toAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.ExistsAsync(file : CloudFile) : Async<bool> =
        CloudFile.Exists(file) |> toAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.Exists(path : string) : bool = 
        c.ExistsAsync(path) |> toSync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.Exists(file : CloudFile) : bool =
        c.ExistsAsync(file) |> toSync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.DeleteAsync(path : string) : Async<unit> = 
        CloudFile.Delete(path) |> toAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.DeleteAsync(file : CloudFile) : Async<unit> =
        CloudFile.Delete(file) |> toAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Input file.</param>
    member c.Delete(path : string) : unit = 
        c.DeleteAsync(path) |> toSync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="file">Input file.</param>
    member c.Delete(file : CloudFile) : unit =
        c.DeleteAsync(file) |> toSync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    member c.CreateAsync(serializer : Stream -> Async<unit>, ?path : string) : Async<CloudFile> = 
        CloudFile.Create(serializer, ?path = path) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    member c.CreateAsync(serializer : Stream -> Async<unit>, directory : string, fileName : string) : Async<CloudFile> = 
        CloudFile.Create(serializer, directory, fileName) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="path">Path to file. Defaults to auto-generated path.</param>
    member c.Create(serializer : Stream -> Async<unit>, ?path : string) : CloudFile = 
        c.CreateAsync(serializer, ?path = path) |> toSync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="serializer">Serializer function.</param>
    /// <param name="directory">Containing directory.</param>
    /// <param name="fileName">File name.</param>
    member c.Create(serializer : Stream -> Async<unit>, directory : string, fileName : string) : CloudFile = 
        c.CreateAsync(serializer, directory, fileName) |> toSync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.ReadAsync<'T>(path : string, deserializer : Stream -> Async<'T>) : Async<'T> = 
        CloudFile.Read<'T>(path, deserializer) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.ReadAsync<'T>(file : CloudFile, deserializer : Stream -> Async<'T>) : Async<'T> = 
        c.ReadAsync(file.Path, deserializer)

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.Read<'T>(path : string, deserializer : Stream -> Async<'T>) : 'T = 
        c.ReadAsync<'T>(path, deserializer) |> toSync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.Read<'T>(file : CloudFile, deserializer : Stream -> Async<'T>) : 'T = 
        c.ReadAsync<'T>(file.Path, deserializer) |> toSync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    member c.EnumerateAsync(?directory : string) : Async<CloudFile []> = 
        CloudFile.Enumerate(?directory = directory) |> toAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="directory">Path to directory. Defaults to the process directory.</param>
    member c.Enumerate(?directory : string) : CloudFile [] = 
        c.EnumerateAsync(?directory = directory) |> toSync

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    member c.WriteLinesAsync(lines : seq<string>, ?encoding : Encoding, ?path : string) : Async<CloudFile> = 
        CloudFile.WriteLines(lines, ?encoding = encoding, ?path = path) |> toAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    member c.WriteLines(lines : seq<string>, ?encoding : Encoding, ?path : string) : CloudFile = 
        c.WriteLinesAsync(lines, ?encoding = encoding, ?path = path) |> toSync


    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLinesAsync(file : string, ?encoding : Encoding) : Async<string []> =
        CloudFile.ReadLines(file, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLines(file : string, ?encoding : Encoding) : string [] =
        c.ReadLinesAsync(file, ?encoding = encoding) |> toSync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLinesAsync(file : CloudFile, ?encoding : Encoding) : Async<string []> = 
        c.ReadLinesAsync(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLines(file : CloudFile, ?encoding : Encoding) : string [] = 
        c.ReadLines(file.Path, ?encoding = encoding)


    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllTextAsync(text : string, ?encoding : Encoding, ?path : string) : Async<CloudFile> = 
        CloudFile.WriteAllText(text, ?encoding = encoding, ?path = path) |> toAsync

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllText(text : string, ?encoding : Encoding, ?path : string) : CloudFile = 
        __.WriteAllTextAsync(text, ?encoding = encoding, ?path = path) |> toSync


    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllTextAsync(file : string, ?encoding : Encoding) : Async<string> =
        CloudFile.ReadAllText(file, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllText(file : string, ?encoding : Encoding) : string =
        c.ReadAllTextAsync(file, ?encoding = encoding) |> toSync


    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllTextAsync(file : CloudFile, ?encoding : Encoding) : Async<string> =
        __.ReadAllTextAsync(file.Path, ?encoding = encoding)

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllText(file : CloudFile, ?encoding : Encoding) : string =
        __.ReadAllTextAsync(file.Path, ?encoding = encoding) |> toSync




    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllBytesAsync(buffer : byte [], ?path : string) : Async<CloudFile> =
       CloudFile.WriteAllBytes(buffer, ?path = path) |> toAsync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    member __.WriteAllBytes(buffer : byte [], ?path : string) : CloudFile =
       __.WriteAllBytesAsync(buffer, ?path = path) |> toSync
        
        
    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.ReadAllBytesAsync(path : string) : Async<byte []> =
        CloudFile.ReadAllBytes(path) |> toAsync

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Input file.</param>
    member __.ReadAllBytes(path : string) : byte [] =
        __.ReadAllBytesAsync(path) |> toSync


    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.ReadAllBytesAsync(file : CloudFile) : Async<byte []> =
        __.ReadAllBytesAsync(file.Path)

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="file">Input file.</param>
    member __.ReadAllBytes(file : CloudFile) : byte [] =
        __.ReadAllBytesAsync(file.Path) |> toSync


[<Sealed; AutoSerializable(false)>]
type FileStoreClient internal (registry : ResourceRegistry) =
    let config = registry.Resolve<CloudFileStoreConfiguration>()

    // TODO : CloudFile & CloudSeq clients
    let pathClient = new CloudPathClient(config)
    let directoryClient = new CloudDirectoryClient(registry)
    let fileClient = new CloudFileClient(registry)

    /// CloudFileStore client.
    member __.File = fileClient
    /// CloudDirectory client.
    member __.Directory = directoryClient
    /// CloudFile client.
    member __.Path = pathClient 


    /// <summary>
    /// Create a new FileStoreClient instance from given resources.
    /// Resources must contain CloudFileStoreConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new FileStoreClient(resources)

[<Sealed; AutoSerializable(false)>]
/// Collection of CloudRef operations.
type CloudRefClient internal (registry : ResourceRegistry) =
    let config = registry.Resolve<CloudFileStoreConfiguration>()
    
    let toAsync (wf : Cloud<'T>) : Async<'T> = Cloud.ToAsync(wf, registry)
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new cloud reference to the underlying store with provided value.
    ///     Cloud references are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud reference value.</param>
    /// <param name="directory">FileStore directory used for cloud ref. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    member __.NewAsync(value : 'T, ?directory : string, ?serializer : ISerializer) =
        CloudRef.New(value, ?directory = directory, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Creates a new cloud reference to the underlying store with provided value.
    ///     Cloud references are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud reference value.</param>
    /// <param name="directory">FileStore directory used for cloud ref. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    member __.New(value : 'T, ?directory : string, ?serializer : ISerializer) =
        __.NewAsync(value, ?directory = directory, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Parses a cloud ref of given type with provided serializer. If successful, returns the cloud ref instance.
    /// </summary>
    /// <param name="path">Path to cloud ref.</param>
    /// <param name="serializer">Serializer for cloud ref.</param>
    member __.ParseAsync<'T>(path : string, ?serializer : ISerializer) = 
        CloudRef.Parse(path, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Parses a cloud ref of given type with provided serializer. If successful, returns the cloud ref instance.
    /// </summary>
    /// <param name="path">Path to cloud ref.</param>
    /// <param name="serializer">Serializer for cloud ref.</param>
    member __.Parse<'T>(path : string, ?serializer : ISerializer) = 
        __.ParseAsync(path, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    member __.ReadAsync(cloudRef : CloudRef<'T>) : Async<'T> = 
        CloudRef.Read(cloudRef) |> toAsync

    /// <summary>
    ///     Dereference a Cloud reference.
    /// </summary>
    /// <param name="cloudRef">CloudRef to be dereferenced.</param>
    member __.Read(cloudRef : CloudRef<'T>) : 'T = 
        __.ReadAsync(cloudRef) |> toSync


    /// <summary>
    ///     Cache a cloud reference to local execution context
    /// </summary>
    /// <param name="cloudRef">Cloud ref input</param>
    member __.CacheAsync(cloudRef : CloudRef<'T>) : Async<bool> = 
        CloudRef.Cache(cloudRef) |> toAsync

    /// <summary>
    ///     Cache a cloud reference to local execution context
    /// </summary>
    /// <param name="cloudRef">Cloud ref input</param>
    member __.Cache(cloudRef : CloudRef<'T>) : bool = 
        __.CacheAsync(cloudRef) |> toSync


[<Sealed; AutoSerializable(false)>]
/// Common client operations on CloudAtom, CloudChannel and CloudFile primitives.
type StoreClient internal (registry : ResourceRegistry) =
    let atomClient    = lazy CloudAtomClient(registry)
    let channelClient = lazy CloudChannelClient(registry)
    let fileStore     = lazy FileStoreClient(registry)

    /// CloudAtom client.
    member __.Atom = atomClient.Value
    /// CloudChannel client.
    member __.Channel = channelClient.Value
    /// CloudFileStore client.
    member __.FileStore = fileStore.Value

    /// <summary>
    /// Create a new StoreClient instance from given resources.
    /// Resources must contain CloudFileStoreConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new StoreClient(resources)