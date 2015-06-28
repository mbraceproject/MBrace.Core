namespace MBrace.Client

#nowarn "0444"

open System.IO
open System.Text

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime
open MBrace.Store
open MBrace.Store.Internals

[<AutoOpen>]
module internal ClientUtils =

    let toLocalAsync resources wf = async {
        let! ct = Async.CancellationToken
        return! Cloud.ToAsync(wf, resources, new InMemoryCancellationToken(ct))
    }

    let toSync resources wf = Cloud.RunSynchronously(wf, resources, new InMemoryCancellationToken())

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
type CloudQueueClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let _ = registry.Resolve<CloudQueueConfiguration>()
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="container">Container for cloud queue.</param>
    member c.CreateAsync<'T>(?container : string) : Async<ICloudQueue<'T>> = 
        CloudQueue.New<'T>(?container = container) |> toAsync

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="container">Container for cloud queue.</param>
    member c.Create<'T>(?container : string) : ICloudQueue<'T> = 
        c.CreateAsync<'T>(?container = container) |> toSync

    /// <summary>
    ///     Asynchronously enqueues a new message to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="message">Message to enqueue.</param>
    member c.EnqueueAsync<'T> (queue : ICloudQueue<'T>, message : 'T) : Async<unit> = 
        CloudQueue.Enqueue<'T> (queue, message) |> toAsync

    /// <summary>
    ///     Enqueues a new message to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="message">Message to enqueue.</param>
    member c.Enqueue<'T> (queue : ICloudQueue<'T>, message : 'T) : unit = 
        c.EnqueueAsync<'T>(queue, message) |> toSync

    /// <summary>
    ///     Asynchronously batch enqueues a sequence of messages to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="messages">Messages to enqueue.</param>
    member c.EnqueueBatchAsync<'T> (queue : ICloudQueue<'T>, messages : seq<'T>) : Async<unit> =
        CloudQueue.EnqueueBatch<'T>(queue, messages) |> toAsync

    /// <summary>
    ///     Batch enqueues a sequence of messages to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="messages">Messages to enqueue.</param>
    member c.EnqueueBatch<'T> (queue : ICloudQueue<'T>, messages : seq<'T>) : Async<unit> =
        CloudQueue.EnqueueBatch<'T>(queue, messages) |> toAsync

    /// <summary>
    ///     Asynchronously dequeues a message from the queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member c.DequeueAsync<'T> (queue : ICloudQueue<'T>, ?timeout : int) : Async<'T> = 
        CloudQueue.Dequeue(queue, ?timeout = timeout) |> toAsync

    /// <summary>
    ///     Dequeues a message from the queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member c.Dequeue<'T> (queue : ICloudQueue<'T>, ?timeout : int) : 'T = 
        c.DequeueAsync(queue, ?timeout = timeout) |> toSync

    /// <summary>
    ///     Asynchronously attempt to dequeue message from queue.
    ///     Returns instantly, with None if empty or Some element if found.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    member c.TryDequeueAsync<'T> (queue : ICloudQueue<'T>) : Async<'T option> =
        CloudQueue.TryDequeue(queue) |> toAsync

    /// <summary>
    ///     Attempt to dequeue message from queue.
    ///     Returns instantly, with None if empty or Some element if found.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    member c.TryDequeue<'T> (queue : ICloudQueue<'T>) : 'T option =
        c.TryDequeueAsync(queue) |> toSync

    /// <summary>
    ///     Deletes the provided queue instance.
    /// </summary>
    /// <param name="queue">Queue to be deleted.</param>
    member c.DeleteAsync(queue : ICloudQueue<'T>) : Async<unit> = 
        CloudQueue.Delete queue |> toAsync

    /// <summary>
    ///     Deletes the provided queue instance.
    /// </summary>
    /// <param name="queue">Queue to be deleted.</param>    
    member c.Delete(queue : ICloudQueue<'T>) : unit = c.DeleteAsync queue |> toSync

    /// <summary>
    ///     Deletes the provided queue container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainerAsync (container : string): Async<unit> = 
        CloudQueue.DeleteContainer container |> toAsync

    /// <summary>
    ///     Deletes the provided queue container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainer (container : string) : unit = 
        c.DeleteContainerAsync container |> toSync

    /// <summary>
    /// Create a new FileStoreClient instance from given resources.
    /// Resources must contain CloudQueueConfiguration value.
    /// </summary>
    /// <param name="resources"></param>
    static member CreateFromResources(resources : ResourceRegistry) =
        new CloudQueueClient(resources)

[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudDictionary API
type CloudDictionaryClient internal (registry : ResourceRegistry) =
    // force exception in event of missing resource
    let _ = registry.Resolve<ICloudDictionaryProvider>()
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
    ///     Asynchronously adds or updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdateAsync (key : string) (updater : 'T option -> 'T) (dictionary : ICloudDictionary<'T>) : Async<'T> =
        CloudDictionary.AddOrUpdate key updater dictionary |> toAsync

    /// <summary>
    ///     Adds or Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdate (key : string) (updater : 'T option -> 'T) (dictionary : ICloudDictionary<'T>) : 'T =
        __.AddOrUpdateAsync key updater dictionary |> toSync

    /// <summary>
    ///     Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.UpdateAsync (key : string) (updater : 'T -> 'T) (dictionary : ICloudDictionary<'T>) : Async<'T> =
        CloudDictionary.Update key updater dictionary |> toAsync

    /// <summary>
    ///     Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Update (key : string) (updater : 'T -> 'T) (dictionary : ICloudDictionary<'T>) : 'T =
        __.UpdateAsync key updater dictionary |> toSync

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
type CloudPathClient internal (registry : ResourceRegistry) =
    let config = registry.Resolve<CloudFileStoreConfiguration>()

    /// <summary>
    ///     Default store directory used by store configuration.
    /// </summary>
    member __.DefaultDirectory = config.DefaultDirectory

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

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    member __.GetRandomFilePath(?container:string) = CloudPath.GetRandomFileName(?container = container) |> toSync registry

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
    /// <param name="dirPath">Path to directory.</param>
    member c.CreateAsync(dirPath : string) : Async<CloudDirectory> =
        CloudDirectory.Create(dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Create(dirPath : string) : CloudDirectory =
        c.CreateAsync(dirPath = dirPath) |> toSync

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
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudDirectory []> = 
        CloudDirectory.Enumerate(dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.Enumerate(dirPath : string) : CloudDirectory [] = 
        c.EnumerateAsync(dirPath = dirPath) |> toSync

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
    /// <param name="path">Path to file.</param>
    /// <param name="serializer">Serializer function.</param>
    member c.CreateAsync(path : string, serializer : Stream -> Async<unit>) : Async<CloudFile> = 
        CloudFile.Create(path, serializer) |> toAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="serializer">Serializer function.</param>
    member c.Create(path : string, serializer : Stream -> Async<unit>) : CloudFile = 
        c.CreateAsync(path, serializer) |> toSync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.ReadAsync<'T>(path : string, deserializer : Stream -> Async<'T>) : Async<'T> = 
        CloudFile.Read<'T>(path, deserializer) |> toAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.Read<'T>(path : string, deserializer : Stream -> Async<'T>) : 'T = 
        c.ReadAsync<'T>(path, deserializer) |> toSync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudFile []> = 
        CloudFile.Enumerate(dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Enumerate(dirPath : string) : CloudFile [] = 
        c.EnumerateAsync(dirPath = dirPath) |> toSync

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLinesAsync(path : string, lines : seq<string>, ?encoding : Encoding) : Async<CloudFile> = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to CloudFile.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLines(path : string, lines : seq<string>, ?encoding : Encoding) : CloudFile = 
        c.WriteAllLinesAsync(path, lines, ?encoding = encoding) |> toSync


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
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllTextAsync(path : string, text : string, ?encoding : Encoding) : Async<CloudFile> = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllText(path : string, text : string, ?encoding : Encoding) : CloudFile = 
        __.WriteAllTextAsync(path, text, ?encoding = encoding) |> toSync


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
    /// <param name="path">Path to file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytesAsync(path : string, buffer : byte []) : Async<CloudFile> =
       CloudFile.WriteAllBytes(path, buffer) |> toAsync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytes(path : string, buffer : byte []) : CloudFile =
       __.WriteAllBytesAsync(path, buffer) |> toSync
        
        
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
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.UploadAsync(sourcePath : string, targetPath : string, ?overwrite : bool) : Async<CloudFile> =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> toAsync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Upload(sourcePath : string, targetPath : string, ?overwrite : bool) : CloudFile =
        __.UploadAsync(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> toSync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.UploadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Async<CloudFile []> =
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> toAsync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Upload(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : CloudFile [] = 
        __.UploadAsync(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> toSync

    /// <summary>
    ///     Asynchronously downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.DownloadAsync(sourcePath : string, targetPath : string, ?overwrite : bool) : Async<unit> =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> toAsync

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Download(sourcePath : string, targetPath : string, ?overwrite : bool) : unit =
        __.DownloadAsync(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> toSync

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.DownloadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Async<string []> =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> toAsync

    /// <summary>
    ///     Downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Download(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : string [] =
        __.DownloadAsync(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> toSync

[<Sealed; AutoSerializable(false)>]
/// Collection of CloudValue operations.
type CloudValueClient internal (registry : ResourceRegistry) =
    let _ = registry.Resolve<CloudFileStoreConfiguration>()
    
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new cloud value to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud value value.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.NewAsync(value : 'T, ?path : string, ?serializer : ISerializer, ?enableCache : bool) =
        CloudValue.New(value, ?path = path, ?serializer = serializer, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Creates a new cloud value to the underlying store with provided value.
    ///     Cloud cells are immutable and cached locally for performance.
    /// </summary>
    /// <param name="value">Cloud value value.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used for object serialization. Defaults to runtime context.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.New(value : 'T, ?path : string, ?serializer : ISerializer, ?enableCache : bool) =
        __.NewAsync(value, ?path = path, ?serializer = serializer, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFileAsync<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool, ?enableCache : bool) =
        CloudValue.OfCloudFile(path, ?deserializer = deserializer, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFile<'T>(path : string, ?deserializer : Stream -> 'T, ?force : bool, ?enableCache : bool) : CloudValue<'T> =
        __.OfCloudFileAsync(path, ?deserializer = deserializer, ?force = force, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFileAsync<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache : bool) = 
        CloudValue.OfCloudFile(path, serializer = serializer, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to cloud file.</param>
    /// <param name="deserializer">Value deserializer function. Defaults to runtime serializer.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache : bool) = 
        __.OfCloudFileAsync(path, serializer = serializer, ?force = force, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided text deserializer and encoding.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFileAsync<'T>(path : string, textDeserializer : TextReader -> 'T, ?force : bool, ?enableCache : bool) =
        CloudValue.OfCloudFile(path, textDeserializer = textDeserializer, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudValue from provided cloud file path with user-provided text deserializer and encoding.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to true.</param>
    member __.OfCloudFile<'T>(path : string, textDeserializer : TextReader -> 'T, ?force : bool, ?enableCache : bool) : CloudValue<'T> =
        __.OfCloudFileAsync(path, textDeserializer = textDeserializer, ?force = force, ?enableCache = enableCache) |> toSync

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
    let _ = registry.Resolve<CloudFileStoreConfiguration>()
    
    let toAsync (wf : Local<'T>) : Async<'T> = toLocalAsync registry wf
    let toSync (wf : Async<'T>) : 'T = Async.RunSync wf

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enables implicit, on-demand caching of instance value. Defaults to false.</param>
    member __.NewAsync(values : seq<'T>, ?path : string, ?serializer : ISerializer, ?enableCache : bool) : Async<CloudSequence<'T>> = 
        CloudSequence.New(values, ?path = path, ?serializer = serializer, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enables implicit, on-demand caching of instance value. Defaults to false.</param>
    member __.New(values : seq<'T>, ?path : string, ?serializer : ISerializer, ?enableCache : bool) : CloudSequence<'T> = 
        __.NewAsync(values, ?path = path, ?serializer = serializer, ?enableCache = enableCache) |> toSync


    /// <summary>
    ///     Creates a collection of partitioned cloud sequences by persisting provided sequence as cloud files in the underlying store.
    ///     A new partition will be appended to the collection as soon as the 'maxPartitionSize' is exceeded in bytes.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per cloud sequence partition.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.NewPartitionedAsync(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer, ?enableCache : bool) : Async<CloudSequence<'T> []> =
        CloudSequence.NewPartitioned(values, maxPartitionSize, ?directory = directory, ?serializer = serializer, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Creates a collection of partitioned cloud sequences by persisting provided sequence as cloud files in the underlying store.
    ///     A new partition will be appended to the collection as soon as the 'maxPartitionSize' is exceeded in bytes.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per cloud sequence partition.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.NewPartitioned(values : seq<'T>, maxPartitionSize, ?directory, ?serializer, ?enableCache : bool) : CloudSequence<'T> [] =
        __.NewPartitionedAsync(values, maxPartitionSize, ?directory = directory, ?serializer = serializer, ?enableCache = enableCache) |> toSync


    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFileAsync<'T>(path : string, ?deserializer : Stream -> seq<'T>, ?force : bool, ?enableCache : bool) : Async<CloudSequence<'T>> = 
        CloudSequence.OfCloudFile<'T>(path, ?deserializer = deserializer, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFile<'T>(path : string, ?deserializer : Stream -> seq<'T>, ?force : bool, ?enableCache : bool) : CloudSequence<'T> = 
        __.OfCloudFileAsync<'T>(path, ?deserializer = deserializer, ?force = force, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFileAsync<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache : bool) : Async<CloudSequence<'T>> = 
        CloudSequence.OfCloudFile<'T>(path, serializer, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache : bool) : CloudSequence<'T> = 
        __.OfCloudFileAsync<'T>(path, serializer, ?force = force, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFileAsync<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, ?encoding : Encoding, ?force : bool, ?enableCache : bool) =
        CloudSequence.OfCloudFile(path, textDeserializer = textDeserializer, ?encoding = encoding, ?force = force, ?enableCache = enableCache) |> toAsync

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    member __.OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, ?encoding : Encoding, ?force : bool, ?enableCache : bool) : CloudSequence<'T> =
        __.OfCloudFileAsync(path, textDeserializer = textDeserializer, ?encoding = encoding, ?force = force, ?enableCache = enableCache) |> toSync

    /// <summary>
    ///     Returns an enumerable that lazily fetches sequence elements from store.
    /// </summary>
    /// <param name="cloudSeq">Cloud sequence to be enumerated.</param>
    member __.ToEnumerableAsync<'T>(cloudSeq : CloudSequence<'T>) : Async<seq<'T>> =
        cloudSeq.ToEnumerable() |> toAsync

    /// <summary>
    ///     Returns an enumerable that lazily fetches sequence elements from store.
    /// </summary>
    /// <param name="cloudSeq">Cloud sequence to be enumerated.</param>
    member __.ToEnumerable(cloudSeq : CloudSequence<'T>) : seq<'T> =
        __.ToEnumerableAsync(cloudSeq) |> toSync

    /// <summary>
    ///     Asynchronously aggregates sequence elements to a local array.
    /// </summary>
    /// <param name="cloudSeq">Cloud sequence to be evaluated.</param>
    member __.ToArrayAsync<'T>(cloudSeq : CloudSequence<'T>) : Async<'T[]> =
        cloudSeq.ToArray() |> toAsync

    /// <summary>
    ///     Aggregates sequence elements to a local array.
    /// </summary>
    /// <param name="cloudSeq">Cloud sequence to be evaluated.</param>
    member __.ToArray<'T>(cloudSeq : CloudSequence<'T>) : 'T[] =
        __.ToArrayAsync<'T>(cloudSeq) |> toSync

/// Client-side API for cloud store operations
[<Sealed; AutoSerializable(false)>]
type CloudStoreClient internal (registry : ResourceRegistry) =
    let atomClient       = lazy CloudAtomClient(registry)
    let queueClient    = lazy CloudQueueClient(registry)
    let dictClient       = lazy CloudDictionaryClient(registry)
    let dirClient        = lazy CloudDirectoryClient(registry)
    let pathClient       = lazy CloudPathClient(registry)
    let fileClient       = lazy CloudFileClient(registry)
    let cloudValueClient = lazy CloudValueClient(registry)
    let cloudseqClient   = lazy CloudSequenceClient(registry)

    /// CloudAtom client.
    member __.Atom = atomClient.Value
    /// CloudQueue client.
    member __.Queue = queueClient.Value
    /// CloudDictionary client.
    member __.Dictionary = dictClient.Value
    /// CloudFile client.
    member __.File = fileClient.Value
    /// CloudDirectory client.
    member __.Directory = dirClient.Value
    /// CloudPath client.
    member __.Path = pathClient.Value
    /// CloudValue client.
    member __.CloudValue = cloudValueClient.Value
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
        new CloudStoreClient(resources)