namespace MBrace.Core.Internals

#nowarn "0444"

open System.IO
open System.ComponentModel
open System.Diagnostics
open System.Text
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Library

/// Collection of CloudValue operations.
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudValueClient (provider : ICloudValueProvider) =
    [<DataMember(Name = "CloudValueProvider")>]
    let provider = provider
    [<IgnoreDataMember>]
    let mutable resources = resource { yield provider }
    [<OnDeserialized>]
    let _onDeserializer (_ : StreamingContext) = resources <- resource { yield provider }

    let toAsync x = Cloud.ToAsync(x, resources)
    let toSync x = Cloud.RunSynchronously(x, resources)

    /// Creates a CloudValue client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudValueClient(resources.Resolve())

    member __.Id = provider.Id
    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewAsync(value : 'T, ?storageLevel : StorageLevel) : Async<CloudValue<'T>> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> toAsync

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.New(value : 'T, ?storageLevel : StorageLevel) : CloudValue<'T> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> toSync

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArrayAsync(values : seq<'T>, ?storageLevel : StorageLevel) : Async<CloudArray<'T>> = 
        CloudValue.NewArray(values, ?storageLevel = storageLevel) |> toAsync

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArray(values : seq<'T>, ?storageLevel : StorageLevel) : CloudArray<'T> = 
        CloudValue.NewArray(values, ?storageLevel = storageLevel) |> toSync

    /// <summary>
    ///     Dereferences a Cloud value.
    /// </summary>
    /// <param name="cloudValue">CloudValue to be dereferenced.</param>
    member __.ReadAsync(cloudValue : CloudValue<'T>) : Async<'T> = 
        CloudValue.Read(cloudValue) |> toAsync

    /// <summary>
    ///     Dereferences a Cloud value.
    /// </summary>
    /// <param name="cloudValue">CloudValue to be dereferenced.</param>
    member __.Read(cloudValue : CloudValue<'T>) : 'T = 
        CloudValue.Read(cloudValue) |> toSync


/// Collection of client methods for CloudAtom API
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudAtomClient (provider : ICloudAtomProvider) =
    [<DataMember(Name = "CloudAtomProvider")>]
    let provider = provider
    [<IgnoreDataMember>]
    let mutable resources = resource { yield provider }
    [<OnDeserialized>]
    let _onDeserializer (_ : StreamingContext) = resources <- resource { yield provider }

    let toAsync x = Cloud.ToAsync(x, resources)
    let toSync x = Cloud.RunSynchronously(x, resources)

    /// Creates a CloudAtom client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudAtomClient(resources.Resolve())

    member __.Id = provider.Id
    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// <summary>
    ///     Asynchronously creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    member c.CreateAsync<'T>(initial : 'T, ?atomId : string, ?container : string) : Async<CloudAtom<'T>> =
        CloudAtom.New(initial, ?atomId = atomId, ?container = container) |> toAsync

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    member c.Create<'T>(initial : 'T, ?atomId : string, ?container : string) : CloudAtom<'T> =
        CloudAtom.New(initial, ?atomId = atomId, ?container = container) |> toSync

    /// <summary>
    ///     Asynchronously attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    member c.GetByIdAsync<'T>(atomId : string, ?container : string) : Async<CloudAtom<'T>> =
        CloudAtom.GetById(atomId, ?container = container) |> toAsync

    /// <summary>
    ///     Attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    member c.GetById<'T>(atomId : string, ?container : string) : CloudAtom<'T> =
        CloudAtom.GetById(atomId, ?container = container) |> toSync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.DeleteAsync (atom : CloudAtom<'T>) : Async<unit> = 
        CloudAtom.Delete atom |> toAsync

    /// <summary>
    ///     Deletes the provided atom instance from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    member c.Delete (atom : CloudAtom<'T>) : unit = 
        CloudAtom.Delete atom |> toSync

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
        CloudAtom.DeleteContainer container |> toSync

    /// <summary>
    ///     Checks if value is supported by current table store.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    member __.IsSupportedValue(value : 'T) : bool = 
        provider.IsSupportedValue value


/// Collection of client methods for CloudAtom API
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudQueueClient (provider : ICloudQueueProvider) =
    [<DataMember(Name = "CloudQueueProvider")>]
    let provider = provider
    [<IgnoreDataMember>]
    let mutable resources = resource { yield provider }
    [<OnDeserialized>]
    let _onDeserializer (_ : StreamingContext) = resources <- resource { yield provider }

    let toAsync x = Cloud.ToAsync(x, resources)
    let toSync x = Cloud.RunSynchronously(x, resources)

    member __.Id = provider.Id
    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// Creates a CloudQueue client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudQueueClient(resources.Resolve())

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier. Defaults to randomly generated name.</param>
    member c.CreateAsync<'T>(?queueId : string) : Async<CloudQueue<'T>> = 
        CloudQueue.New<'T>(?queueId = queueId) |> toAsync

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier. Defaults to randomly generated name.</param>
    member c.Create<'T>(?queueId : string) : CloudQueue<'T> = 
        CloudQueue.New<'T>(?queueId = queueId) |> toSync

    /// <summary>
    ///     Attempt to recover an existing queue of given type and identifier.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier.</param>
    member c.GetByIdAsync(queueId : string) : Async<CloudQueue<'T>> =
        CloudQueue.GetById(queueId) |> toAsync

    /// <summary>
    ///     Attempt to recover an existing queue of given type and identifier.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier.</param>
    member c.GetById(queueId : string) : CloudQueue<'T> =
        CloudQueue.GetById(queueId) |> toSync


/// Collection of client methods for CloudDictionary API
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudDictionaryClient (provider : ICloudDictionaryProvider) =
    [<DataMember(Name = "CloudDictionaryProvider")>]
    let provider = provider
    [<IgnoreDataMember>]
    let mutable resources = resource { yield provider }
    [<OnDeserialized>]
    let _onDeserializer (_ : StreamingContext) = resources <- resource { yield provider }

    let toAsync x = Cloud.ToAsync(x, resources)
    let toSync x = Cloud.RunSynchronously(x, resources)

    member __.Id = provider.Id
    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// Creates a CloudDictionary client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudDictionaryClient(resources.Resolve())

    /// <summary>
    ///     Asynchronously creates a new CloudDictionary instance.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier. Defaults to randomly generated name.</param>
    member __.NewAsync<'T> (?dictionaryId : string) : Async<CloudDictionary<'T>> = 
        CloudDictionary.New<'T> (?dictionaryId = dictionaryId) |> toAsync

    /// <summary>
    ///    Creates a new CloudDictionary instance.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier. Defaults to randomly generated name.</param>
    member __.New<'T> (?dictionaryId : string) : CloudDictionary<'T> =
        CloudDictionary.New<'T> (?dictionaryId = dictionaryId) |> toSync

    /// <summary>
    ///     Asynchronously attempt to recover an already existing CloudDictionary of provided Id and type.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    member c.GetByIdAsync<'T>(dictionaryId : string) : Async<CloudDictionary<'T>> =
        CloudDictionary.GetById<'T>(dictionaryId) |> toAsync

    /// <summary>
    ///     Attempt to recover an already existing CloudDictionary of provided Id and type.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    member c.GetById<'T>(dictionaryId : string) : CloudDictionary<'T> =
        CloudDictionary.GetById<'T>(dictionaryId) |> toSync


/// Collection of path-related file store methods.
[<Sealed; AutoSerializable(true)>]
type CloudPathClient internal (fileStore : ICloudFileStore, resources : ResourceRegistry) =

    let run x = Cloud.RunSynchronously(x, resources = resources)

    /// Default store directory used by store configuration.
    member __.DefaultDirectory : string = fileStore.DefaultDirectory

    /// Gets the root directory used by the store instance.
    member __.RootDirectory : string = fileStore.RootDirectory

    /// Gets whether the store instance uses case sensitive paths.
    member __.IsCaseSensitive : bool = fileStore.IsCaseSensitiveFileSystem

    /// <summary>
    ///     Returns the directory name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetDirectoryName(path : string) : string = fileStore.GetDirectoryName path

    /// <summary>
    ///     Returns the file name for given path.
    /// </summary>
    /// <param name="path">Input file path.</param>
    member __.GetFileName(path : string) : string = fileStore.GetFileName path

    /// <summary>
    ///     Combines two strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    member __.Combine(path1 : string, path2 : string) : string = fileStore.Combine [| path1 ; path2 |]

    /// <summary>
    ///     Combines three strings into one path.
    /// </summary>
    /// <param name="path1">First path.</param>
    /// <param name="path2">Second path.</param>
    /// <param name="path3">Third path.</param>
    member __.Combine(path1 : string, path2 : string, path3 : string) : string = fileStore.Combine [| path1 ; path2 ; path3 |]

    /// <summary>
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    member __.Combine(paths : string []) : string = fileStore.Combine paths
                   
    /// Generates a random, uniquely specified path to directory
    member __.GetRandomDirectoryName() : string = fileStore.GetRandomDirectoryName()

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    member __.GetRandomFilePath(?container:string) : string = CloudPath.GetRandomFileName(?container = container) |> run


/// Collection of file store operations
[<Sealed; AutoSerializable(true)>]
type CloudDirectoryClient internal (resources : ResourceRegistry) =
    let toAsync x = Cloud.ToAsync(x, resources)
    let run x = Cloud.RunSynchronously(x, resources = resources)
    
    /// <summary>
    ///     Checks if directory path exists in given path.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.ExistsAsync(dirPath : string) : Async<bool> = 
        CloudDirectory.Exists dirPath |> toAsync

    /// <summary>
    ///     Checks if directory exists in given path
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Exists(dirPath : string) : bool = 
        CloudDirectory.Exists dirPath |> run

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.CreateAsync(dirPath : string) : Async<CloudDirectoryInfo> =
        CloudDirectory.Create dirPath |> toAsync

    /// <summary>
    ///     Creates a new directory in store.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Create(dirPath : string) : CloudDirectoryInfo =
        CloudDirectory.Create dirPath |> run

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
        CloudDirectory.Delete(dirPath, ?recursiveDelete = recursiveDelete) |> run

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudDirectoryInfo []> = 
        CloudDirectory.Enumerate(dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Enumerates all directories contained in path.
    /// </summary>
    /// <param name="dirPath">Path to directory to be enumerated.</param>
    member c.Enumerate(dirPath : string) : CloudDirectoryInfo [] = 
        CloudDirectory.Enumerate(dirPath = dirPath) |> run

/// Collection of file store operations
[<Sealed; AutoSerializable(true)>]
type CloudFileClient internal (resources : ResourceRegistry) =
    let toAsync x = Cloud.ToAsync(x, resources)
    let toSync x = Cloud.RunSynchronously(x, resources = resources)

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
        CloudFile.GetSize(path) |> toSync

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
        CloudFile.Exists(path) |> toSync

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
        CloudFile.Delete(path) |> toSync

    /// <summary>
    ///     Asynchronously creates a new file in store and returns a local writer stream.
    /// </summary>
    /// <param name="path">Path to file.</param>
    member c.BeginWriteAsync(path : string) : Async<System.IO.Stream> = 
        CloudFile.BeginWrite path |> toAsync

    /// <summary>
    ///     Creates a new file in store and returns a local writer stream.
    /// </summary>
    /// <param name="path">Path to file.</param>
    member c.BeginWrite(path : string) : System.IO.Stream = 
        CloudFile.BeginWrite path |> toSync

    /// <summary>
    ///     Asynchronously returns a reader function for given path in cloud store, if it exists.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.BeginReadAsync(path : string) : Async<System.IO.Stream> = 
        CloudFile.BeginRead(path) |> toAsync

    /// <summary>
    ///     Returns a reader function for given path in cloud store, if it exists.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    member c.BeginRead(path : string) : System.IO.Stream = 
        CloudFile.BeginRead(path) |> toSync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.EnumerateAsync(dirPath : string) : Async<CloudFileInfo []> = 
        CloudFile.Enumerate(dirPath = dirPath) |> toAsync

    /// <summary>
    ///     Gets all files that exist in given container.
    /// </summary>
    /// <param name="dirPath">Path to directory.</param>
    member c.Enumerate(dirPath : string) : CloudFileInfo [] = 
        CloudFile.Enumerate(dirPath = dirPath) |> toSync

    //
    //  Cloud file text utilities
    //

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLinesAsync(path : string, lines : seq<string>, ?encoding : Encoding) : Async<CloudFileInfo> = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to CloudFile.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLines(path : string, lines : seq<string>, ?encoding : Encoding) : CloudFileInfo = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> toSync


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
        CloudFile.ReadLines(path, ?encoding = encoding) |> toSync

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
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> toSync


    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllTextAsync(path : string, text : string, ?encoding : Encoding) : Async<CloudFileInfo> = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllText(path : string, text : string, ?encoding : Encoding) : CloudFileInfo = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> toSync


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
        CloudFile.ReadAllText(path, ?encoding = encoding) |> toSync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytesAsync(path : string, buffer : byte []) : Async<CloudFileInfo> =
       CloudFile.WriteAllBytes(path, buffer) |> toAsync

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="buffer">Source buffer.</param>
    member __.WriteAllBytes(path : string, buffer : byte []) : CloudFileInfo =
       CloudFile.WriteAllBytes(path, buffer) |> toSync
        
        
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
        CloudFile.ReadAllBytes(path) |> toSync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.UploadAsync(sourcePath : string, targetPath : string, ?overwrite : bool, ?compress : bool) : Async<CloudFileInfo> =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?compress = compress) |> toAsync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.Upload(sourcePath : string, targetPath : string, ?overwrite : bool, ?compress : bool) : CloudFileInfo =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?compress = compress) |> toSync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.UploadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?compress : bool) : Async<CloudFileInfo []> =
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?compress = compress) |> toAsync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.Upload(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?compress : bool) : CloudFileInfo [] = 
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?compress = compress) |> toSync

    /// <summary>
    ///     Asynchronously downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.DownloadAsync(sourcePath : string, targetPath : string, ?overwrite : bool, ?decompress : bool) : Async<unit> =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?decompress = decompress) |> toAsync

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.Download(sourcePath : string, targetPath : string, ?overwrite : bool, ?decompress : bool) : unit =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?decompress = decompress) |> toSync

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.DownloadAsync(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?decompress : bool) : Async<string []> =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?decompress = decompress) |> toAsync

    /// <summary>
    ///     Downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.Download(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool, ?decompress : bool) : string [] =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?decompress = decompress) |> toSync

    /// <summary>
    ///     Asynchronously persists a value to the cloud store.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="path">Path to persist file. Defaults to randomly generated path.</param>
    /// <param name="serializer">Serializer to be used. Defaults to execution context serializer.</param>
    /// <param name="compress">Compress serialization. Defaults to false.</param>
    member __.PersistAsync(value : 'T, ?path : string, ?serializer : ISerializer, ?compress : bool) : Async<PersistedValue<'T>> =
        PersistedValue.New(value, ?path = path, ?serializer = serializer, ?compress = compress) |> toAsync

    /// <summary>
    ///     Persists a value to the cloud store.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="path">Path to persist file. Defaults to randomly generated path.</param>
    /// <param name="serializer">Serializer to be used. Defaults to execution context serializer.</param>
    /// <param name="compress">Compress serialization. Defaults to false.</param>
    member __.Persist(value : 'T, ?path : string, ?serializer : ISerializer, ?compress : bool) : PersistedValue<'T> =
        PersistedValue.New(value, ?path = path, ?serializer = serializer, ?compress = compress) |> toSync

    /// <summary>
    ///     Asynchronously creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    member __.PersistSequenceAsync(values : seq<'T>, ?path : string, ?serializer : ISerializer, ?compress : bool) : Async<PersistedSequence<'T>> =
        PersistedSequence.New(values, ?path = path, ?serializer = serializer, ?compress = compress) |> toAsync

    /// <summary>
    ///     Creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    member __.PersistSequence(values : seq<'T>, ?path : string, ?serializer : ISerializer, ?compress : bool) : PersistedSequence<'T> =
        PersistedSequence.New(values, ?path = path, ?serializer = serializer, ?compress = compress) |> toSync

/// Serializable CloudFileSystem instance object
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudFileSystem (fileStore : ICloudFileStore, ?serializer : ISerializer) =
    [<DataMember(Name = "CloudFileStore")>]
    let fileStore = fileStore
    [<DataMember(Name = "Serializer")>]
    let serializer : ISerializer option = serializer

    let mutable pathClient = Unchecked.defaultof<_>
    let mutable dirClient = Unchecked.defaultof<_>
    let mutable fileClient = Unchecked.defaultof<_>

    let init () = 
        let resources = resource { yield fileStore ; match serializer with Some s -> yield s | None -> () }
        pathClient <- new CloudPathClient(fileStore, resources)
        dirClient <- new CloudDirectoryClient(resources)
        fileClient <- new CloudFileClient(resources)

    do init()

    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) = init ()

    /// Creates a CloudFileSystem client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudFileSystem(resources.Resolve(), ?serializer = resources.TryResolve())

    /// CloudPath client.
    member __.Path = pathClient
    /// CloudDirectory client.
    member __.Directory = dirClient
    /// CloudFile client.
    member __.File = fileClient

    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Store = fileStore
    [<DebuggerBrowsable(DebuggerBrowsableState.Never); EditorBrowsable(EditorBrowsableState.Never)>]
    member __.Serializer = serializer

    member __.Id = fileStore.Id
    override __.ToString() = fileStore.Id

/// NonSerializable global store client object
[<AutoSerializable(false)>]
type CloudStoreClient (resources : ResourceRegistry) =
    let fileSystem = lazy(CloudFileSystem.Create resources)
    let catom = lazy(CloudAtomClient.Create resources)
    let cqueue = lazy(CloudQueueClient.Create resources)
    let cvalue = lazy(CloudValueClient.Create resources)
    let cdict = lazy(CloudDictionaryClient.Create resources)

    /// Gets the default CloudFileSystem client instance
    member __.CloudFileSystem = fileSystem.Value
    /// Gets the default CloudAtom client instance
    member __.CloudAtom = catom.Value
    /// Gets the default CloudQueue client instance
    member __.CloudQueue = cqueue.Value
    /// Gets the default CloudValue client instance
    member __.CloudValue = cvalue.Value
    /// Gets the default CloudDictionary client instance
    member __.CloudDictionary = cdict.Value

namespace MBrace.Core

open MBrace.Core.Internals

/// API for cloud store operations
type CloudStore private () =

    /// Gets the default CloudFileSystem client instance from the execution context
    static member FileSystem = local {
        let! resources = Cloud.GetResourceRegistry()
        return CloudFileSystem.Create resources
    }

    /// Gets the default CloudAtom client instance from the execution context
    static member CloudAtom = local {
        let! resources = Cloud.GetResourceRegistry()
        return CloudAtomClient.Create resources
    }

    /// Gets the default CloudQueue client instance from the execution context
    static member CloudQueue = local {
        let! resources = Cloud.GetResourceRegistry()
        return CloudQueueClient.Create resources
    }

    /// Gets the default CloudValue client instance from the execution context
    static member CloudValue = local {
        let! resources = Cloud.GetResourceRegistry()
        return CloudValueClient.Create resources
    }

    /// Gets the default CloudDictionary client instance from the execution context
    static member CloudDictionary = local {
        let! resources = Cloud.GetResourceRegistry()
        return CloudDictionaryClient.Create resources
    }