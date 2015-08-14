namespace MBrace.Runtime

#nowarn "0444"

open System.IO
open System.Text

open MBrace.Core
open MBrace.Core.Internals
open MBrace.ThreadPool

[<Sealed; AutoSerializable(false)>]
/// Collection of CloudValue operations.
type CloudValueClient internal (runtime : ThreadPoolClient) =
    let _ = runtime.Resources.Resolve<CloudFileStoreConfiguration>()

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewAsync(value : 'T, ?storageLevel : StorageLevel) : Async<CloudValue<'T>> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.New(value : 'T, ?storageLevel : StorageLevel) : CloudValue<'T> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> runtime.RunSynchronously

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArrayAsync(values : seq<'T>, ?storageLevel : StorageLevel) : Async<CloudArray<'T>> = 
        CloudValue.NewArray(values, ?storageLevel = storageLevel) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArray(values : seq<'T>, ?storageLevel : StorageLevel) : CloudArray<'T> = 
        CloudValue.NewArray(values, ?storageLevel = storageLevel) |> runtime.RunSynchronously

    /// <summary>
    ///     Dereferences a Cloud value.
    /// </summary>
    /// <param name="cloudValue">CloudValue to be dereferenced.</param>
    member __.ReadAsync(cloudValue : CloudValue<'T>) : Async<'T> = 
        CloudValue.Read(cloudValue) |> runtime.ToAsync

    /// <summary>
    ///     Dereferences a Cloud value.
    /// </summary>
    /// <param name="cloudValue">CloudValue to be dereferenced.</param>
    member __.Read(cloudValue : CloudValue<'T>) : 'T = 
        CloudValue.Read(cloudValue) |> runtime.RunSynchronously

/// Collection of client methods for CloudAtom API
[<Sealed; AutoSerializable(false)>]
type CloudAtomClient internal (runtime : ThreadPoolClient) =

    // force exception in event of missing resource
    let config = runtime.Resources.Resolve<CloudAtomConfiguration>()

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member c.CreateAsync<'T>(initial : 'T, ?container : string) : Async<CloudAtom<'T>> =
        CloudAtom.New(initial, ?container = container) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    member c.Create<'T>(initial : 'T, ?container : string) : CloudAtom<'T> =
        CloudAtom.New(initial, ?container = container) |> runtime.RunSynchronously
       
    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member c.ReadAsync(atom : CloudAtom<'T>) : Async<'T> = 
        CloudAtom.Read(atom) |> runtime.ToAsync

    /// <summary>
    ///     Dereferences a cloud atom.
    /// </summary>
    /// <param name="atom">Atom instance.</param>
    member c.Read(atom : CloudAtom<'T>) : 'T = 
        CloudAtom.Read(atom) |> runtime.RunSynchronously

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.UpdateAsync (atom : CloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int): Async<unit> =
        CloudAtom.Update(atom, updater, ?maxRetries = maxRetries) |> runtime.ToAsync

    /// <summary>
    ///     Atomically updates the contained value.
    /// </summary>
    /// <param name="updater">value updating function.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.Update (atom : CloudAtom<'T>, updater : 'T -> 'T, ?maxRetries : int): unit = 
        CloudAtom.Update(atom, updater, ?maxRetries = maxRetries) |> runtime.RunSynchronously

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member c.ForceAsync (atom : CloudAtom<'T>, value : 'T) : Async<unit> =
        CloudAtom.Force(atom, value) |> runtime.ToAsync

    /// <summary>
    ///     Forces the contained value to provided argument.
    /// </summary>
    /// <param name="value">Value to be set.</param>
    /// <param name="atom">Atom instance to be updated.</param>
    member c.Force (atom : CloudAtom<'T>, value : 'T) : unit = 
        CloudAtom.Force(atom, value) |> runtime.RunSynchronously

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="transactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.TransactAsync (atom : CloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : Async<'R> =
        CloudAtom.Transact(atom, transactF, ?maxRetries = maxRetries) |> runtime.ToAsync

    /// <summary>
    ///     Transactionally updates the contained value.
    /// </summary>
    /// <param name="atom">Atom instance to be updated.</param>
    /// <param name="transactF">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries before giving up. Defaults to infinite.</param>
    member c.Transact (atom : CloudAtom<'T>, transactF : 'T -> 'R * 'T, ?maxRetries : int) : 'R = 
        CloudAtom.Transact(atom, transactF, ?maxRetries = maxRetries) |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.DeleteAsync (atom : CloudAtom<'T>) : Async<unit> = 
        CloudAtom.Delete atom |> runtime.ToAsync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.Delete (atom : CloudAtom<'T>) : unit = 
        CloudAtom.Delete atom |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes the provided atom container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainerAsync (container : string) : Async<unit> = 
        CloudAtom.DeleteContainer container |> runtime.ToAsync

    /// <summary>
    ///     Deletes the provided atom container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainer (container : string) : unit = 
        CloudAtom.DeleteContainer container |> runtime.RunSynchronously

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    member __.IsSupportedValue(value : 'T) : bool = 
        config.AtomProvider.IsSupportedValue value


[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudAtom API
type CloudQueueClient internal (runtime : ThreadPoolClient) =
    // force exception in event of missing resource
    let _ = runtime.Resources.Resolve<CloudQueueConfiguration>()

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="container">Container for cloud queue.</param>
    member c.CreateAsync<'T>(?container : string) : Async<CloudQueue<'T>> = 
        CloudQueue.New<'T>(?container = container) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="container">Container for cloud queue.</param>
    member c.Create<'T>(?container : string) : CloudQueue<'T> = 
        CloudQueue.New<'T>(?container = container) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously enqueues a new message to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="message">Message to enqueue.</param>
    member c.EnqueueAsync<'T> (queue : CloudQueue<'T>, message : 'T) : Async<unit> = 
        CloudQueue.Enqueue<'T> (queue, message) |> runtime.ToAsync

    /// <summary>
    ///     Enqueues a new message to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="message">Message to enqueue.</param>
    member c.Enqueue<'T> (queue : CloudQueue<'T>, message : 'T) : unit = 
        CloudQueue.Enqueue<'T> (queue, message) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously batch enqueues a sequence of messages to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="messages">Messages to enqueue.</param>
    member c.EnqueueBatchAsync<'T> (queue : CloudQueue<'T>, messages : seq<'T>) : Async<unit> =
        CloudQueue.EnqueueBatch<'T>(queue, messages) |> runtime.ToAsync

    /// <summary>
    ///     Batch enqueues a sequence of messages to the queue.
    /// </summary>
    /// <param name="queue">Target queue.</param>
    /// <param name="messages">Messages to enqueue.</param>
    member c.EnqueueBatch<'T> (queue : CloudQueue<'T>, messages : seq<'T>) : unit =
        CloudQueue.EnqueueBatch<'T>(queue, messages) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously dequeues a message from the queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member c.DequeueAsync<'T> (queue : CloudQueue<'T>, ?timeout : int) : Async<'T> = 
        CloudQueue.Dequeue(queue, ?timeout = timeout) |> runtime.ToAsync

    /// <summary>
    ///     Dequeues a message from the queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member c.Dequeue<'T> (queue : CloudQueue<'T>, ?timeout : int) : 'T = 
        CloudQueue.Dequeue(queue, ?timeout = timeout) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously attempt to dequeue message from queue.
    ///     Returns instantly, with None if empty or Some element if found.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    member c.TryDequeueAsync<'T> (queue : CloudQueue<'T>) : Async<'T option> =
        CloudQueue.TryDequeue(queue) |> runtime.ToAsync

    /// <summary>
    ///     Attempt to dequeue message from queue.
    ///     Returns instantly, with None if empty or Some element if found.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    member c.TryDequeue<'T> (queue : CloudQueue<'T>) : 'T option =
        CloudQueue.TryDequeue(queue) |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes the provided queue instance.
    /// </summary>
    /// <param name="queue">Queue to be deleted.</param>
    member c.DeleteAsync(queue : CloudQueue<'T>) : Async<unit> = 
        CloudQueue.Delete queue |> runtime.ToAsync

    /// <summary>
    ///     Deletes the provided queue instance.
    /// </summary>
    /// <param name="queue">Queue to be deleted.</param>    
    member c.Delete(queue : CloudQueue<'T>) : unit = 
        CloudQueue.Delete queue |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes the provided queue container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainerAsync (container : string): Async<unit> = 
        CloudQueue.DeleteContainer container |> runtime.ToAsync

    /// <summary>
    ///     Deletes the provided queue container and all its contents.
    /// </summary>
    /// <param name="container">Container name.</param>
    member c.DeleteContainer (container : string) : unit = 
        CloudQueue.DeleteContainer container |> runtime.RunSynchronously


[<Sealed; AutoSerializable(false)>]
/// Collection of client methods for CloudDictionary API
type CloudDictionaryClient internal (runtime : ThreadPoolClient) =

    // force exception in event of missing resource
    let _ = runtime.Resources.Resolve<ICloudDictionaryProvider>()

    /// Asynchronously creates a new CloudDictionary instance.
    member __.NewAsync<'T> () : Async<CloudDictionary<'T>> = 
        CloudDictionary.New<'T> () |> runtime.ToAsync

    /// Creates a new CloudDictionary instance.
    member __.New<'T> () : CloudDictionary<'T> =
        CloudDictionary.New<'T> () |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously checks if entry of given key exists in dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="dictionary">Dictionary to be checked.</param>
    member __.ContainsKeyAsync (key : string) (dictionary : CloudDictionary<'T>) : Async<bool> =
        CloudDictionary.ContainsKey key dictionary |> runtime.ToAsync

    /// <summary>
    ///     Checks if entry of given key exists in dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="dictionary">Dictionary to be checked.</param>
    member __.ContainsKey (key : string) (dictionary : CloudDictionary<'T>) : bool =
        CloudDictionary.ContainsKey key dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddAsync (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) : Async<unit> =
        CloudDictionary.Add key value dictionary |> runtime.ToAsync

    /// <summary>
    ///     Adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Add (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) : unit =
        CloudDictionary.Add key value dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.TryAddAsync (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) : Async<bool> =
        CloudDictionary.TryAdd key value dictionary |> runtime.ToAsync

    /// <summary>
    ///     Adds key/value entry to dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="value">Value to entry.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.TryAdd (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) : bool =
        CloudDictionary.TryAdd key value dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously adds or updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdateAsync (key : string) (updater : 'T option -> 'T) (dictionary : CloudDictionary<'T>) : Async<'T> =
        CloudDictionary.AddOrUpdate key updater dictionary |> runtime.ToAsync

    /// <summary>
    ///     Adds or Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.AddOrUpdate (key : string) (updater : 'T option -> 'T) (dictionary : CloudDictionary<'T>) : 'T =
        CloudDictionary.AddOrUpdate key updater dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.UpdateAsync (key : string) (updater : 'T -> 'T) (dictionary : CloudDictionary<'T>) : Async<'T> =
        CloudDictionary.Update key updater dictionary |> runtime.ToAsync

    /// <summary>
    ///     Updates a key/value entry on a dictionary.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Value updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Update (key : string) (updater : 'T -> 'T) (dictionary : CloudDictionary<'T>) : 'T =
        CloudDictionary.Update key updater dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously removes an entry of given id from dictionary.
    /// </summary>
    /// <param name="key">Key for entry to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.RemoveAsync (key : string) (dictionary : CloudDictionary<'T>) : Async<bool> =
        CloudDictionary.Remove key dictionary |> runtime.ToAsync

    /// <summary>
    ///     Removes an entry of given id from dictionary.
    /// </summary>
    /// <param name="key">Key for entry to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    member __.Remove (key : string) (dictionary : CloudDictionary<'T>) : bool =
        CloudDictionary.Remove key dictionary |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously try reading value of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be looked up.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    member __.TryFindAsync (key : string) (dictionary : CloudDictionary<'T>) : Async<'T option> =
        CloudDictionary.TryFind key dictionary |> runtime.ToAsync

    /// <summary>
    ///     Try reading value of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be looked up.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    member __.TryFind (key : string) (dictionary : CloudDictionary<'T>) : 'T option =
        CloudDictionary.TryFind key dictionary |> runtime.RunSynchronously


[<Sealed; AutoSerializable(false)>]
/// Collection of path-related file store methods.
type CloudPathClient internal (runtime : ThreadPoolClient) =
    let config = runtime.Resources.Resolve<CloudFileStoreConfiguration>()

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
    member __.GetRandomFilePath(?container:string) = CloudPath.GetRandomFileName(?container = container) |> runtime.RunSynchronously


/// Collection of file store operations
[<Sealed; AutoSerializable(false)>]
type CloudDirectoryClient internal (runtime : ThreadPoolClient) =

    let _ = runtime.Resources.Resolve<CloudFileStoreConfiguration>()
    
    /// <summary>
    ///     Checks if directory path exists in given path.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.ExistsAsync(dirPath : string) : Async<bool> = 
        CloudDirectory.Exists(dirPath) |> runtime.ToAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Exists(dirPath : string) : bool = 
        CloudDirectory.Exists(dirPath) |> runtime.RunSynchronously

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.CreateAsync(dirPath : string) : Async<CloudDirectory> =
        CloudDirectory.Create(dirPath = dirPath) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Create(dirPath : string) : CloudDirectory =
        CloudDirectory.Create(dirPath = dirPath) |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Path to directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.DeleteAsync(dirPath : string, ?recursiveDelete : bool) : Async<unit> = 
        CloudDirectory.Delete(dirPath, ?recursiveDelete = recursiveDelete) |> runtime.ToAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Path to directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.Delete(dirPath : string, ?recursiveDelete : bool) : unit = 
        CloudDirectory.Delete(dirPath, ?recursiveDelete = recursiveDelete) |> runtime.RunSynchronously

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudDirectory []> = 
        CloudDirectory.Enumerate(dirPath = dirPath) |> runtime.ToAsync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.Enumerate(dirPath : string) : CloudDirectory [] = 
        CloudDirectory.Enumerate(dirPath = dirPath) |> runtime.RunSynchronously

[<Sealed; AutoSerializable(false)>]
/// Collection of file store operations
type CloudFileClient internal (runtime : ThreadPoolClient) =
    let _ = runtime.Resources.Resolve<CloudFileStoreConfiguration> ()

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.GetSizeAsync(path : string) : Async<int64> = 
        CloudFile.GetSize(path) |> runtime.ToAsync

    /// <summary>
    ///     Gets the size of provided file, in bytes.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.GetSize(path : string) : int64 = 
        CloudFile.GetSize(path) |> runtime.RunSynchronously

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.ExistsAsync(path : string) : Async<bool> = 
        CloudFile.Exists(path) |> runtime.ToAsync

    /// <summary>
    ///     Checks if file exists in store.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.Exists(path : string) : bool = 
        CloudFile.Exists(path) |> runtime.RunSynchronously

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.DeleteAsync(path : string) : Async<unit> = 
        CloudFile.Delete(path) |> runtime.ToAsync

    /// <summary>
    ///     Deletes file in given path.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.Delete(path : string) : unit = 
        CloudFile.Delete(path) |> runtime.RunSynchronously

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="serializer">Serializer function.</param>
    member c.CreateAsync(path : string, serializer : Stream -> Async<unit>) : Async<CloudFile> = 
        CloudFile.Create(path, serializer) |> runtime.ToAsync

    /// <summary>
    ///     Creates a new file in store with provided serializer function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="serializer">Serializer function.</param>
    member c.Create(path : string, serializer : Stream -> Async<unit>) : CloudFile = 
        CloudFile.Create(path, serializer) |> runtime.RunSynchronously

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.ReadAsync<'T>(path : string, deserializer : Stream -> Async<'T>) : Async<'T> = 
        CloudFile.Read<'T>(path, deserializer) |> runtime.ToAsync

    /// <summary>
    ///     Reads file in store with provided deserializer function.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="deserializer">Deserializer function.</param>
    member c.Read<'T>(path : string, deserializer : Stream -> Async<'T>) : 'T = 
        CloudFile.Read<'T>(path, deserializer) |> runtime.RunSynchronously

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudFile []> = 
        CloudFile.Enumerate(dirPath = dirPath) |> runtime.ToAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Enumerate(dirPath : string) : CloudFile [] = 
        CloudFile.Enumerate(dirPath = dirPath) |> runtime.RunSynchronously

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
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> runtime.ToAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to CloudFile.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLines(path : string, lines : seq<string>, ?encoding : Encoding) : CloudFile = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> runtime.RunSynchronously


    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLinesAsync(path : string, ?encoding : Encoding) : Async<string seq> =
        CloudFile.ReadLines(path, ?encoding = encoding) |> runtime.ToAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLines(path : string, ?encoding : Encoding) : seq<string> =
        CloudFile.ReadLines(path, ?encoding = encoding) |> runtime.RunSynchronously

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLinesAsync(path : string, ?encoding : Encoding) : Async<string []> =
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> runtime.ToAsync

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLines(path : string, ?encoding : Encoding) : string [] =
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> runtime.RunSynchronously


    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllTextAsync(path : string, text : string, ?encoding : Encoding) : Async<CloudFile> = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> runtime.ToAsync

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllText(path : string, text : string, ?encoding : Encoding) : CloudFile = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> runtime.RunSynchronously


    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllTextAsync(path : string, ?encoding : Encoding) : Async<string> =
        CloudFile.ReadAllText(path, ?encoding = encoding) |> runtime.ToAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllText(path : string, ?encoding : Encoding) : string =
        CloudFile.ReadAllText(path, ?encoding = encoding) |> runtime.RunSynchronously

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytesAsync(path : string, buffer : byte []) : Async<CloudFile> =
       CloudFile.WriteAllBytes(path, buffer) |> runtime.ToAsync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytes(path : string, buffer : byte []) : CloudFile =
       CloudFile.WriteAllBytes(path, buffer) |> runtime.RunSynchronously
        
        
    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member __.ReadAllBytesAsync(path : string) : Async<byte []> =
        CloudFile.ReadAllBytes(path) |> runtime.ToAsync

    /// <summary>
    ///     Store all contents of given file to a new byte array.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member __.ReadAllBytes(path : string) : byte [] =
        CloudFile.ReadAllBytes(path) |> runtime.RunSynchronously

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.UploadAsync(sourcePath : string, targetPath : string, ?overwrite : bool) : Async<CloudFile> =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> runtime.ToAsync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Upload(sourcePath : string, targetPath : string, ?overwrite : bool) : CloudFile =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> runtime.RunSynchronously

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.UploadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Async<CloudFile []> =
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> runtime.ToAsync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Upload(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : CloudFile [] = 
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.DownloadAsync(sourcePath : string, targetPath : string, ?overwrite : bool) : Async<unit> =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> runtime.ToAsync

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Download(sourcePath : string, targetPath : string, ?overwrite : bool) : unit =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite) |> runtime.RunSynchronously

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.DownloadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Async<string []> =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> runtime.ToAsync

    /// <summary>
    ///     Downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member __.Download(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : string [] =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite) |> runtime.RunSynchronously

/// Client-side API for cloud store operations
[<Sealed; AutoSerializable(false)>]
type CloudStoreClient internal (runtime : ThreadPoolClient) =
    let atomClient       = lazy CloudAtomClient(runtime)
    let queueClient    = lazy CloudQueueClient(runtime)
    let dictClient       = lazy CloudDictionaryClient(runtime)
    let dirClient        = lazy CloudDirectoryClient(runtime)
    let pathClient       = lazy CloudPathClient(runtime)
    let fileClient       = lazy CloudFileClient(runtime)
    let cloudValueClient = lazy CloudValueClient(runtime)

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
    /// Gets the associated ResourceRegistry.
    member __.Resources = runtime.Resources

    /// <summary>
    ///     Create a new StoreClient instance that targets provided in-memory runtime.
    /// </summary>
    /// <param name="runtime">In-Memory runtime driver.</param>
    static member Create(runtime : ThreadPoolClient) =
        new CloudStoreClient(runtime)