namespace MBrace.Core.Internals

#nowarn "0444"

open System
open System.IO
open System.ComponentModel
open System.Text
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Library

/// Collection of Serialization utilities.
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type SerializationClient (serializer : ISerializer, textSerializer : ITextSerializer option) =
    [<DataMember(Name = "Serializer")>]
    let serializer = serializer
    [<DataMember(Name = "TextSerializer")>]
    let textSerializer = textSerializer

    let getTextSerializer () =
        match serializer with
        | :? ITextSerializer as ts -> ts
        | _ ->
            match textSerializer with
            | Some ts -> ts
            | None -> raise <| ResourceNotFoundException("ITextSerializer")

    /// Creates a Serializer client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new SerializationClient(resources.Resolve(), resources.TryResolve())

    member __.Id = serializer.Id
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Serializer = serializer
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.TextSerializer = textSerializer
    override __.ToString() = serializer.Id

    /// <summary>
    ///     Quickly computes the size of a serializable object graph in bytes.
    /// </summary>
    /// <param name="graph">Serializable object graph to be computed.</param>
    member __.ComputeObjectSize<'T>(graph : 'T) : int64 = serializer.ComputeObjectSize(graph)

    /// <summary>
    ///     Creates an in-memory clone of supplied serializable object graph.
    /// </summary>
    /// <param name="graph">Serializable object graph to be cloned.</param>
    member __.Clone<'T>(graph : 'T) : 'T = serializer.Clone(graph)

    /// <summary>
    ///     Serializes provided object graph to underlying write stream.
    /// </summary>
    /// <param name="stream">Stream to serialize object.</param>
    /// <param name="graph">Object graph to be serialized.</param>
    /// <param name="leaveOpen">Leave open stream after serialization. Defaults to false.</param>
    member __.Serialize<'T>(stream : Stream, graph : 'T, [<O;D(null:obj)>]?leaveOpen : bool) : unit =
        serializer.Serialize(stream, graph, defaultArg leaveOpen false)

    /// <summary>
    ///     Deserializes provided object graph from underlying read stream.
    /// </summary>
    /// <param name="stream">Stream to deserialize object from.</param>
    /// <param name="leaveOpen">Leave open stream after deserialization. Defaults to false.</param>
    member __.Deserialize<'T>(stream : Stream, [<O;D(null:obj)>]?leaveOpen : bool) : 'T =
        serializer.Deserialize<'T>(stream, defaultArg leaveOpen false)

    /// <summary>
    ///     Serializes provided object graph to underlying text writer.
    /// </summary>
    /// <param name="target">Target text writer.</param>
    /// <param name="graph">Object graph to be serialized.</param>
    /// <param name="leaveOpen">Leave open writer after serialization. Defaults to false.</param>
    member __.TextSerialize<'T>(target : TextWriter, graph : 'T, [<O;D(null:obj)>]?leaveOpen : bool) : unit =
        getTextSerializer().TextSerialize(target, graph, defaultArg leaveOpen false) 

    /// <summary>
    ///     Deserializes object graph from underlying text reader.
    /// </summary>
    /// <param name="source">Source text reader.</param>
    /// <param name="leaveOpen">Leave open writer after deserialization. Defaults to false.</param>
    member __.TextDeserialize<'T>(source : TextReader, [<O;D(null:obj)>]?leaveOpen : bool) : LocalCloud<'T> =
        getTextSerializer().TextDeserialize(source, defaultArg leaveOpen false)

    /// <summary>
    ///     Serializes provided object graph to byte array.
    /// </summary>
    /// <param name="graph">Object graph to be serialized.</param>
    member __.Pickle<'T>(graph : 'T) : byte [] =
        use mem = new MemoryStream()
        do serializer.Serialize(mem, graph, true)
        mem.ToArray()

    /// <summary>
    ///     Deserializes object from given byte array pickle.
    /// </summary>
    /// <param name="pickle">Input serialization bytes.</param>
    member __.UnPickle<'T>(pickle : byte[]) : 'T =
        use mem = new MemoryStream(pickle)
        serializer.Deserialize<'T>(mem, true)

    /// <summary>
    ///     Serializes provided object graph to string.
    /// </summary>
    /// <param name="graph">Graph to be serialized.</param>
    member __.PickleToString<'T>(graph : 'T) : string =
        let serializer = getTextSerializer()
        use sw = new StringWriter()
        serializer.TextSerialize(sw, graph, true)
        sw.ToString()

    /// <summary>
    ///     Deserializes object from given string pickle.
    /// </summary>
    /// <param name="pickle">Input serialization string.</param>
    member __.UnPickleOfString<'T>(pickle : string) : 'T =
        let serializer = getTextSerializer()
        use sr = new StringReader(pickle)
        serializer.TextDeserialize<'T>(sr, true)

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
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewAsync(value : 'T, [<O;D(null:obj)>]?storageLevel : StorageLevel) : Async<CloudValue<'T>> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> toAsync

    /// <summary>
    ///     Creates a new cloud value to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.New(value : 'T, [<O;D(null:obj)>]?storageLevel : StorageLevel) : CloudValue<'T> = 
        CloudValue.New(value, ?storageLevel = storageLevel) |> toSync

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArrayAsync(values : seq<'T>, [<O;D(null:obj)>]?storageLevel : StorageLevel) : Async<CloudArray<'T>> = 
        CloudValue.NewArray(values, ?storageLevel = storageLevel) |> toAsync

    /// <summary>
    ///     Creates a new cloud array to the underlying cache with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel used for cloud value. Defaults to runtime default.</param>
    member __.NewArray(values : seq<'T>, [<O;D(null:obj)>]?storageLevel : StorageLevel) : CloudArray<'T> = 
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
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// <summary>
    ///     Asynchronously creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    member c.CreateAsync<'T>(initial : 'T, [<O;D(null:obj)>]?atomId : string, [<O;D(null:obj)>]?container : string) : Async<CloudAtom<'T>> =
        CloudAtom.New(initial, ?atomId = atomId, ?container = container) |> toAsync

    /// <summary>
    ///     Creates a new cloud atom instance with given value.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    /// <param name="atomId">Cloud atom unique entity identifier. Defaults to randomly generated identifier.</param>
    /// <param name="container">Cloud atom unique entity identifier. Defaults to process container.</param>
    member c.Create<'T>(initial : 'T, [<O;D(null:obj)>]?atomId : string, [<O;D(null:obj)>]?container : string) : CloudAtom<'T> =
        CloudAtom.New(initial, ?atomId = atomId, ?container = container) |> toSync

    /// <summary>
    ///     Asynchronously attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    member c.GetByIdAsync<'T>(atomId : string, [<O;D(null:obj)>]?container : string) : Async<CloudAtom<'T>> =
        CloudAtom.GetById(atomId, ?container = container) |> toAsync

    /// <summary>
    ///     Attempt to recover an existing atom instance by its unique identifier and type.
    /// </summary>
    /// <param name="atomId">CloudAtom unique entity identifier.</param>
    /// <param name="container">Cloud atom container. Defaults to process container.</param>
    member c.GetById<'T>(atomId : string, [<O;D(null:obj)>]?container : string) : CloudAtom<'T> =
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
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// Creates a CloudQueue client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudQueueClient(resources.Resolve())

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier. Defaults to randomly generated name.</param>
    member c.CreateAsync<'T>([<O;D(null:obj)>]?queueId : string) : Async<CloudQueue<'T>> = 
        CloudQueue.New<'T>(?queueId = queueId) |> toAsync

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="queueId">Cloud queue identifier. Defaults to randomly generated name.</param>
    member c.Create<'T>([<O;D(null:obj)>]?queueId : string) : CloudQueue<'T> = 
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
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Provider = provider
    override __.ToString() = provider.Id

    /// Creates a CloudDictionary client by resolving the local execution context.
    static member Create(resources : ResourceRegistry) = new CloudDictionaryClient(resources.Resolve())

    /// <summary>
    ///     Asynchronously creates a new CloudDictionary instance.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier. Defaults to randomly generated name.</param>
    member __.NewAsync<'T> ([<O;D(null:obj)>]?dictionaryId : string) : Async<CloudDictionary<'T>> = 
        CloudDictionary.New<'T> (?dictionaryId = dictionaryId) |> toAsync

    /// <summary>
    ///    Creates a new CloudDictionary instance.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier. Defaults to randomly generated name.</param>
    member __.New<'T> ([<O;D(null:obj)>]?dictionaryId : string) : CloudDictionary<'T> =
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
    ///     Combines an array of paths into a path.
    /// </summary>
    /// <param name="paths">Strings to be combined.</param>
    member __.Combine([<ParamArray>] paths : string []) : string = fileStore.Combine paths
                   
    /// Generates a random, uniquely specified path to directory
    member __.GetRandomDirectoryName() : string = fileStore.GetRandomDirectoryName()

    /// <summary>
    ///     Creates a uniquely defined file path for given container.
    /// </summary>
    /// <param name="container">Path to containing directory. Defaults to process directory.</param>
    member __.GetRandomFilePath([<O;D(null:obj)>]?container:string) : string = CloudPath.GetRandomFileName(?container = container) |> run


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
    member c.DeleteAsync(dirPath : string, [<O;D(null:obj)>]?recursiveDelete : bool) : Async<unit> =
        CloudDirectory.Delete(dirPath, ?recursiveDelete = recursiveDelete) |> toAsync

    /// <summary>
    ///     Deletes directory from store.
    /// </summary>
    /// <param name="dirPath">Path to directory to be deleted.</param>
    /// <param name="recursiveDelete">Delete recursively. Defaults to false.</param>
    member c.Delete(dirPath : string, [<O;D(null:obj)>]?recursiveDelete : bool) : unit = 
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
    ///     Asynchronously Write the contents of a stream directly to a CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to read from. Assumes that the stream is already at the correct position for reading.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member c.UploadFromStreamAsync(path : string, stream : Stream, [<O;D(null:obj)>]?overwrite : bool) : Async<CloudFileInfo> =
        CloudFile.UploadFromStream(path, stream, ?overwrite = overwrite) |> toAsync

    /// <summary>
    ///     Write the contents of a stream directly to a CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to read from. Assumes that the stream is already at the correct position for reading.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    member c.UploadFromStream(path : string, stream : Stream, [<O;D(null:obj)>]?overwrite : bool) : CloudFileInfo =
        CloudFile.UploadFromStream(path, stream, ?overwrite = overwrite) |> toSync

    /// <summary>
    ///     Asynchronously write the contents of a CloudFile directly to a Stream.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to write to.</param>
    member c.DownloadToStreamAsync(path : string, stream : Stream) : Async<unit> =
        CloudFile.DownloadToStream(path, stream) |> toAsync

    /// <summary>
    ///     Asynchronously write the contents of a CloudFile directly to a Stream.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="inputStream">The stream to write to.</param>
    member c.DownloadToStream(path : string, stream : Stream) : unit =
        CloudFile.DownloadToStream(path, stream) |> toSync

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
    member c.WriteAllLinesAsync(path : string, lines : seq<string>, [<O;D(null:obj)>]?encoding : Encoding) : Async<CloudFileInfo> = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="path">Path to CloudFile.</param>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.WriteAllLines(path : string, lines : seq<string>, [<O;D(null:obj)>]?encoding : Encoding) : CloudFileInfo = 
        CloudFile.WriteAllLines(path, lines, ?encoding = encoding) |> toSync


    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLinesAsync(path : string, [<O;D(null:obj)>]?encoding : Encoding) : Async<string seq> =
        CloudFile.ReadLines(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as a sequence of lines.
    /// </summary>
    /// <param name="file">Input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadLines(path : string, [<O;D(null:obj)>]?encoding : Encoding) : seq<string> =
        CloudFile.ReadLines(path, ?encoding = encoding) |> toSync

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLinesAsync(path : string, [<O;D(null:obj)>]?encoding : Encoding) : Async<string []> =
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Reads a file as an array of lines.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllLines(path : string, [<O;D(null:obj)>]?encoding : Encoding) : string [] =
        CloudFile.ReadAllLines(path, ?encoding = encoding) |> toSync


    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllTextAsync(path : string, text : string, [<O;D(null:obj)>]?encoding : Encoding) : Async<CloudFileInfo> = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="path">Path to Cloud file.</param>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    member __.WriteAllText(path : string, text : string, [<O;D(null:obj)>]?encoding : Encoding) : CloudFileInfo = 
        CloudFile.WriteAllText(path, text, ?encoding = encoding) |> toSync


    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member __.ReadAllTextAsync(path : string, [<O;D(null:obj)>]?encoding : Encoding) : Async<string> =
        CloudFile.ReadAllText(path, ?encoding = encoding) |> toAsync

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="path">Path to input file.</param>
    /// <param name="encoding">Text encoding.</param>
    member c.ReadAllText(path : string, [<O;D(null:obj)>]?encoding : Encoding) : string =
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
    member __.UploadAsync(sourcePath : string, targetPath : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?compress : bool) : Async<CloudFileInfo> =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?compress = compress) |> toAsync

    /// <summary>
    ///     Uploads a local file to store.
    /// </summary>
    /// <param name="sourcePath">Local file system path to file.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.Upload(sourcePath : string, targetPath : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?compress : bool) : CloudFileInfo =
        CloudFile.Upload(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?compress = compress) |> toSync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.UploadAsync(sourcePaths : seq<string>, targetDirectory : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?compress : bool) : Async<CloudFileInfo []> =
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?compress = compress) |> toAsync

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="compress">Compress file as uploaded using GzipStream. Defaults to false.</param>
    member __.Upload(sourcePaths : seq<string>, targetDirectory : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?compress : bool) : CloudFileInfo [] = 
        CloudFile.Upload(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?compress = compress) |> toSync

    /// <summary>
    ///     Asynchronously downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.DownloadAsync(sourcePath : string, targetPath : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?decompress : bool) : Async<unit> =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?decompress = decompress) |> toAsync

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Path to file in store.</param>
    /// <param name="targetPath">Path to target file in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.Download(sourcePath : string, targetPath : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?decompress : bool) : unit =
        CloudFile.Download(sourcePath, targetPath = targetPath, ?overwrite = overwrite, ?decompress = decompress) |> toSync

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.DownloadAsync(sourcePaths : seq<string>, targetDirectory : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?decompress : bool) : Async<string []> =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?decompress = decompress) |> toAsync

    /// <summary>
    ///     Downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    /// <param name="decompress">Decompress file as downloaded using GzipStream. Defaults to false.</param>
    member __.Download(sourcePaths : seq<string>, targetDirectory : string, [<O;D(null:obj)>]?overwrite : bool, [<O;D(null:obj)>]?decompress : bool) : string [] =
        CloudFile.Download(sourcePaths, targetDirectory = targetDirectory, ?overwrite = overwrite, ?decompress = decompress) |> toSync

    /// <summary>
    ///     Asynchronously persists a value to the cloud store.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="path">Path to persist file. Defaults to randomly generated path.</param>
    /// <param name="serializer">Serializer to be used. Defaults to execution context serializer.</param>
    /// <param name="compress">Compress serialization. Defaults to false.</param>
    member __.PersistAsync(value : 'T, [<O;D(null:obj)>]?path : string, [<O;D(null:obj)>]?serializer : ISerializer, [<O;D(null:obj)>]?compress : bool) : Async<PersistedValue<'T>> =
        PersistedValue.New(value, ?path = path, ?serializer = serializer, ?compress = compress) |> toAsync

    /// <summary>
    ///     Persists a value to the cloud store.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="path">Path to persist file. Defaults to randomly generated path.</param>
    /// <param name="serializer">Serializer to be used. Defaults to execution context serializer.</param>
    /// <param name="compress">Compress serialization. Defaults to false.</param>
    member __.Persist(value : 'T, [<O;D(null:obj)>]?path : string, [<O;D(null:obj)>]?serializer : ISerializer, [<O;D(null:obj)>]?compress : bool) : PersistedValue<'T> =
        PersistedValue.New(value, ?path = path, ?serializer = serializer, ?compress = compress) |> toSync

    /// <summary>
    ///     Asynchronously creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    member __.PersistSequenceAsync(values : seq<'T>, [<O;D(null:obj)>]?path : string, [<O;D(null:obj)>]?serializer : ISerializer, [<O;D(null:obj)>]?compress : bool) : Async<PersistedSequence<'T>> =
        PersistedSequence.New(values, ?path = path, ?serializer = serializer, ?compress = compress) |> toAsync

    /// <summary>
    ///     Creates a new persisted sequence by writing provided sequence to a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="path">Path to persist cloud value in File Store. Defaults to a random file name.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="compress">Compress value as uploaded using GzipStream. Defaults to false.</param>
    member __.PersistSequence(values : seq<'T>, [<O;D(null:obj)>]?path : string, [<O;D(null:obj)>]?serializer : ISerializer, [<O;D(null:obj)>]?compress : bool) : PersistedSequence<'T> =
        PersistedSequence.New(values, ?path = path, ?serializer = serializer, ?compress = compress) |> toSync

/// Serializable CloudFileSystem instance object
[<Sealed; DataContract; StructuredFormatDisplay("{Id}")>]
type CloudFileSystem (fileStore : ICloudFileStore, [<O;D(null:obj)>]?serializer : ISerializer) =
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

    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Store = fileStore
    [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    member __.Serializer = serializer

    member __.Id = fileStore.Id
    override __.ToString() = fileStore.Id

/// NonSerializable global store client object
[<AutoSerializable(false)>]
type CloudStoreClient (resources : ResourceRegistry) =
    let serializer = lazy(SerializationClient.Create resources)
    let fileSystem = lazy(CloudFileSystem.Create resources)
    let catom = lazy(CloudAtomClient.Create resources)
    let cqueue = lazy(CloudQueueClient.Create resources)
    let cvalue = lazy(CloudValueClient.Create resources)
    let cdict = lazy(CloudDictionaryClient.Create resources)

    /// Gets the default cluster serializer client instance
    member __.Serializer = serializer.Value
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

    /// Gets the default Serialization client instace from the execution context
    static member Serializer = local {
        let! resources = Cloud.GetResourceRegistry()
        return SerializationClient.Create resources
    }

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