namespace MBrace.Client

#nowarn "0444"

open System.IO
open System.Text

open MBrace
open MBrace.Store
open MBrace.Continuation
open MBrace.Runtime.InMemory

[<AutoOpen>]
module internal ClientUtils =

    let toLocalAsync resources wf = async {
        let! ct = Async.CancellationToken
        return! Cloud.ToAsync(wf, resources, new InMemoryCancellationToken(ct))
    }

[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudAtom API
type CloudAtomClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let config = registry.Resolve<CloudAtomConfiguration>()

    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
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
    /// <param name="transactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.TransactAsync (atom : ICloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : Async<'R> =
        CloudAtom.Transact(atom, transactF, ?maxRetries = maxRetries) |> toAsync

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="transactF">Transaction function.</param>
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
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
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
/// Collection of client methods for CloudDictionary API
type CloudDictionaryClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let config = registry.Resolve<ICloudDictionaryProvider>()
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// Asynchronously creates a new CloudDictionary instance.
    member __.NewAsync<'T> () : Async<ICloudDictionary<'T>> = CloudDictionary.New<'T> () |> toAsync

    /// Creates a new CloudDictionary instance.
    member __.New<'T> () : ICloudDictionary<'T> = __.NewAsync<'T> () |> toSync

    /// <summary>
    ///     Asynchronously checks if entry of given key exists in dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="dictionary">Dictionary to be checked.</param>
    member __.ContainsKeyAsync (key : string) (dictionary : ICloudDictionary<'T>) : Async<bool> =
        CloudDictionary.ContainsKey key dictionary |> toAsync

    /// <summary>
    ///     Checks if entry of given key exists in dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="dictionary">Dictionary to be checked.</param>
    member __.ContainsKey (key : string) (dictionary : ICloudDictionary<'T>) : bool =
        __.ContainsKeyAsync key dictionary |> toSync

    /// <summary>
    ///     Asynchronously adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddAsync (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) : Async<unit> =
        CloudDictionary.Add key value dictionary |> toAsync

    /// <summary>
    ///     Adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Add (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) : unit =
        __.AddAsync key value dictionary |> toSync

    /// <summary>
    ///     Asynchronously adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.TryAddAsync (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) : Async<bool> =
        CloudDictionary.TryAdd key value dictionary |> toAsync

    /// <summary>
    ///     Adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.TryAdd (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) : bool =
        __.TryAddAsync key value dictionary |> toSync

    /// <summary>
    ///     Asynchronously updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdateAsync (key : string) (updater : 'T option -> 'T) (dictionary : ICloudDictionary<'T>) : Async<'T> =
        CloudDictionary.AddOrUpdate key updater dictionary |> toAsync

    /// <summary>
    ///     Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdate (key : string) (updater : 'T option -> 'T) (dictionary : ICloudDictionary<'T>) : 'T =
        __.AddOrUpdateAsync key updater dictionary |> toSync

    /// <summary>
    ///     Asynchronously removes an entry of given id from dictionary.
    /// </summary>
    /// <param name="key">Key for entry to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.RemoveAsync (key : string) (dictionary : ICloudDictionary<'T>) : Async<bool> =
        CloudDictionary.Remove key dictionary |> toAsync

    /// <summary>
    ///     Removes an entry of given id from dictionary.
    /// </summary>
    /// <param name="key">Key for entry to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Remove (key : string) (dictionary : ICloudDictionary<'T>) : bool =
        __.RemoveAsync key dictionary |> toSync

    /// <summary>
    ///     Asynchronously try reading value of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be looked up.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    member __.TryFindAsync (key : string) (dictionary : ICloudDictionary<'T>) : Async<'T option> =
        CloudDictionary.TryFind key dictionary |> toAsync

    /// <summary>
    ///     Try reading value of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be looked up.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    member __.TryFind (key : string) (dictionary : ICloudDictionary<'T>) : 'T option =
        __.TryFindAsync key dictionary |> toSync


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

    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf
    
    /// <summary>
    ///     Checks if directory path exists in given path.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.ExistsAsync(dirPath : string) : Async<bool> = 
        CloudDirectory.Exists(dirPath) |> toAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Exists(dirPath : string) : bool = 
        c.ExistsAsync dirPath |> toSync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to randomly generated directory.</param>
    member c.CreateAsync(?dirPath : string) : Async<CloudDirectory> =
        CloudDirectory.Create(?dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to randomly generated directory.</param>
    member c.Create(?dirPath : string) : CloudDirectory =
        c.CreateAsync(?dirPath = dirPath) |> toSync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Path to directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.DeleteAsync(dirPath : string, ?recursiveDelete : bool) : Async<unit> = 
        CloudDirectory.Delete(dirPath, ?recursiveDelete = recursiveDelete) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Path to directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.Delete(dirPath : string, ?recursiveDelete : bool) : unit = 
        c.DeleteAsync(dirPath, ?recursiveDelete = recursiveDelete) |> toSync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated. Defaults to root directory.</param>
    member c.EnumerateAsync(?dirPath : string) : Async<CloudDirectory []> = 
        CloudDirectory.Enumerate(?dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated. Defaults to root directory.</param>
    member c.Enumerate(?dirPath : string) : CloudDirectory [] = 
        c.EnumerateAsync(?dirPath = dirPath) |> toSync

[<Sealed; AutoSerializable(false)>]
/// Collection of file store operations
type CloudFileClient internal (registry : ResourceRegistry) =

    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.GetSizeAsync(path : string) : Async<int64> = 
        CloudFile.GetSize(path) |> toAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.GetSize(path : string) : int64 = 
        c.GetSizeAsync(path) |> toSync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.ExistsAsync(path : string) : Async<bool> = 
        CloudFile.Exists(path) |> toAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.Exists(path : string) : bool = 
        c.ExistsAsync(path) |> toSync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.DeleteAsync(path : string) : Async<unit> = 
        CloudFile.Delete(path) |> toAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.Delete(path : string) : unit = 
        c.DeleteAsync(path) |> toSync

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
    /// <param name="dirPath">Path to containing directory.</param>
    /// <param name="fileName">File name.</param>
    member c.CreateAsync(serializer : Stream -> Async<unit>, dirPath : string, fileName : string) : Async<CloudFile> = 
        CloudFile.Create(serializer, dirPath, fileName) |> toAsync

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
    /// <param name="dirPath">Path to containing directory.</param>
    /// <param name="fileName">File name.</param>
    member c.Create(serializer : Stream -> Async<unit>, dirPath : string, fileName : string) : CloudFile = 
        c.CreateAsync(serializer, dirPath, fileName) |> toSync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="leaveOpen">Leave stream open after deserialization. Defaults to false.</param>
    member c.ReadAsync<'T>(path : string, deserializer : Stream -> Async<'T>, ?leaveOpen : bool) : Async<'T> = 
        CloudFile.Read<'T>(path, deserializer, ?leaveOpen = leaveOpen) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    /// <param name="leaveOpen">Leave stream open after deserialization. Defaults to false.</param>
    member c.Read<'T>(path : string, deserializer : Stream -> Async<'T>, ?leaveOpen : bool) : 'T = 
        c.ReadAsync<'T>(path, deserializer, ?leaveOpen = leaveOpen) |> toSync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to the process directory.</param>
    member c.EnumerateAsync(?dirPath : string) : Async<CloudFile []> = 
        CloudFile.Enumerate(?dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory. Defaults to the process directory.</param>
    member c.Enumerate(?dirPath : string) : CloudFile [] = 
        c.EnumerateAsync(?dirPath = dirPath) |> toSync

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to file.</param>
    member c.WriteAllLinesAsync(lines : seq<string>, ?encoding : Encoding, ?path : string) : Async<CloudFile> = 
        CloudFile.WriteAllLines(lines, ?encoding = encoding, ?path = path) |> toAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    member c.WriteAllLines(lines : seq<string>, ?encoding : Encoding, ?path : string) : CloudFile = 
        c.WriteAllLinesAsync(lines, ?encoding = encoding, ?path = path) |> toSync


    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLinesAsync(path : string, ?encoding : Encoding) : Async<string seq> =
        CloudFile.ReadLines(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLines(path : string, ?encoding : Encoding) : seq<string> =
        c.ReadLinesAsync(path, ?encoding = encoding) |> toSync

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLinesAsync(path : string, ?encoding : Encoding) : Async<string []> =
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLines(path : string, ?encoding : Encoding) : string [] =
        c.ReadAllLinesAsync(path, ?encoding = encoding) |> toSync


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
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllTextAsync(path : string, ?encoding : Encoding) : Async<string> =
        CloudFile.ReadAllText(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllText(path : string, ?encoding : Encoding) : string =
        c.ReadAllTextAsync(path, ?encoding = encoding) |> toSync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to file.</param>
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
    /// <param name="path">Path to input file.</param>
    member __.ReadAllBytesAsync(path : string) : Async<byte []> =
        CloudFile.ReadAllBytes(path) |> toAsync

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member __.ReadAllBytes(path : string) : byte [] =
        __.ReadAllBytesAsync(path) |> toSync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="localFile">Local file system path to file.</param>
    /// <param name="targetDirectory">Containing directory in cloud store. Defaults to process default.</param>
    member __.UploadAsync(localFile : string, ?targetDirectory : string) : Async<CloudFile> =
        CloudFile.Upload(localFile, ?targetDirectory = targetDirectory) |> toAsync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="localFile">Local file system path to file.</param>
    /// <param name="targetDirectory">Containing directory in cloud store. Defaults to process default.</param>
    member __.Upload(localFile : string, ?targetDirectory : string) : CloudFile =
        __.UploadAsync(localFile, ?targetDirectory = targetDirectory) |> toSync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="localFiles">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store. Defaults to process default.</param>
    member __.UploadAsync(localFiles : seq<string>, ?targetDirectory : string) : Async<CloudFile []> =
        local {
            let localFiles = Seq.toArray localFiles
            match localFiles |> Array.tryFind (not << File.Exists) with
            | Some notFound -> raise <| new FileNotFoundException(notFound)
            | None -> ()

            return!
                localFiles
                |> Array.map (fun f -> CloudFile.Upload(f, ?targetDirectory = targetDirectory))
                |> Local.Parallel
        } |> toAsync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="localFiles">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store. Defaults to process default.</param>
    member __.Upload(localFiles : seq<string>, ?targetDirectory : string) : CloudFile [] = 
        __.UploadAsync(localFiles, ?targetDirectory = targetDirectory) |> toSync


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
/// Collection of CloudValue operations.
type CloudCellClient internal (registry : ResourceRegistry) =
    let config = registry.Resolve<CloudFileStoreConfiguration>()
    
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new cloud value to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud value value.</param>
    /// <param name="directory">FileStore directory used for cloud value. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    member __.NewAsync(value : 'T, ?directory : string, ?serializer : ISerializer) =
        CloudValue.New(value, ?directory = directory, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Creates a new cloud value to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud value value.</param>
    /// <param name="directory">FileStore directory used for cloud value. Defaults to execution context setting.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    member __.New(value : 'T, ?directory : string, ?serializer : ISerializer) =
        __.NewAsync(value, ?directory = directory, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Parses a cloud value of given type with provided serializer. If successful, returns the cloud value instance.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer for cloud value.</param>
    member __.FromFileAsync<'T>(path : string, ?serializer : ISerializer) = 
        CloudValue.FromFile(path, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Parses a cloud value of given type with provided serializer. If successful, returns the cloud value instance.
    /// </summary>
    /// <param name="path">Path to cloud value.</param>
    /// <param name="serializer">Serializer for cloud value.</param>
    member __.FromFile<'T>(path : string, ?serializer : ISerializer) = 
        __.FromFileAsync(path, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Dereference a Cloud value.
    /// </summary>
    /// <param name="cloudCell">CloudValue to be dereferenced.</param>
    member __.ReadAsync(cloudCell : CloudValue<'T>) : Async<'T> = 
        CloudValue.Read(cloudCell) |> toAsync

    /// <summary>
    ///     Dereference a Cloud value.
    /// </summary>
    /// <param name="cloudCell">CloudValue to be dereferenced.</param>
    member __.Read(cloudCell : CloudValue<'T>) : 'T = 
        __.ReadAsync(cloudCell) |> toSync


[<Sealed; AutoSerializable(false)>]
/// Collection of CloudValue operations.
type CloudSequenceClient internal (registry : ResourceRegistry) =
    let config = registry.Resolve<CloudFileStoreConfiguration>()
    
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    member __.NewAsync(values : seq<'T>, ?directory, ?serializer) : Async<CloudSequence<'T>> = 
        CloudSequence.New(values, ?directory = directory, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    member __.New(values : seq<'T>, ?directory, ?serializer) : CloudSequence<'T> = 
        __.NewAsync(values, ?directory = directory, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Creates a collection of Cloud sequences partitioned by file size.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per Cloud sequence partition.</param>
    /// <param name="directory"></param>
    /// <param name="serializer"></param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    member __.NewPartitionedAsync(values : seq<'T>, maxPartitionSize, ?directory, ?serializer) : Async<CloudSequence<'T> []> =
        CloudSequence.NewPartitioned(values, maxPartitionSize, ?directory = directory, ?serializer = serializer) |> toAsync

    /// <summary>
    ///     Creates a collection of Cloud sequences partitioned by file size.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per Cloud sequence partition.</param>
    /// <param name="directory"></param>
    /// <param name="serializer"></param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    member __.NewPartitioned(values : seq<'T>, maxPartitionSize, ?directory, ?serializer) : CloudSequence<'T> [] =
        __.NewPartitionedAsync(values, maxPartitionSize, ?directory = directory, ?serializer = serializer) |> toSync


    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    member __.FromFileAsync<'T>(path : string, ?deserializer, ?force) : Async<CloudSequence<'T>> = 
        CloudSequence.FromFile<'T>(path, ?deserializer = deserializer, ?force = force) |> toAsync

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="deserializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    member __.FromFile<'T>(path : string, ?deserializer, ?force) : CloudSequence<'T> = 
        __.FromFileAsync<'T>(path, ?deserializer = deserializer, ?force = force) |> toSync

    /// <summary>
    ///     Creates a CloudSequence from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    member __.FromFileAsync<'T>(path : string, serializer : ISerializer, ?force) : Async<CloudSequence<'T>> = 
        CloudSequence.FromFile<'T>(path, serializer, ?force = force) |> toAsync

    /// <summary>
    ///     Creates a CloudSequence from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="serializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    member __.FromFile<'T>(path : string, serializer : ISerializer, ?force) : CloudSequence<'T> = 
        __.FromFileAsync<'T>(path, serializer, ?force = force) |> toSync


[<Sealed; AutoSerializable(false)>]
/// Common client operations on CloudAtom, CloudChannel and CloudFile primitives.
type StoreClient internal (registry : ResourceRegistry) =
    let atomClient     = lazy CloudAtomClient(registry)
    let channelClient  = lazy CloudChannelClient(registry)
    let dictClient     = lazy CloudDictionaryClient(registry)
    let fileStore      = lazy FileStoreClient(registry)
    let cloudCellClient = lazy CloudCellClient(registry)
    let cloudseqClient = lazy CloudSequenceClient(registry)

    /// CloudAtom client.
    member __.Atom = atomClient.Value
    /// CloudChannel client.
    member __.Channel = channelClient.Value
    /// CloudDictionary client.
    member __.Dictionary = dictClient.Value
    /// CloudFileStore client.
    member __.FileStore = fileStore.Value
    /// CloudValue client.
    member __.CloudValue = cloudCellClient.Value
    /// CloudSequence client.
    member __.CloudSequence = cloudseqClient.Value
    /// Gets the associated ResourceRegistry.
    member __.Resources = registry

    /// <summary>
    /// Create a new StoreClient instance from given resources.
    /// Resources must contain CloudFileStoreConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new StoreClient(resources)
