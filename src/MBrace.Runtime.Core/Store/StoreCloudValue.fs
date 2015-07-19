namespace MBrace.Runtime.Store

open System
open System.Collections.Concurrent
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.FsPickler.Hashing

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Vagabond

// StoreCloudValue instances have three possible representations
//  * Encapsulated: values small enough to be encapsulated in the CloudValue instance
//  * VagabondValue: large values managed by Vagabond and are therefore available in the local context
//  * Cached: values that are cached by the implementation
[<AutoSerializable(true); NoEquality; NoComparison>]
type private CachedEntityId =
    | Encapsulated of value:obj * hash:HashResult
    | VagabondValue of hash:HashResult
    | Cached of hash:HashResult
with
    /// Gets the FsPickler hashcode of the cached value
    member c.Hash =
        match c with
        | Encapsulated (_,hash) -> hash
        | Cached hash -> hash
        | VagabondValue hash -> hash

    /// <summary>
    ///     Creates a cache entity identifier for provided object graph.
    /// </summary>
    /// <param name="obj">Input serializable object graph.</param>
    /// <param name="sizeThreshold">Size threshold in bytes. Objects less than the treshold will be encapsulated in CloudValue instance.</param>
    static member FromObject(obj:obj, sizeThreshold:int64) : CachedEntityId =
        let vgb = VagabondRegistry.Instance
        let hash = vgb.Serializer.ComputeHash obj
        if obj = null || hash.Length <= sizeThreshold then Encapsulated(obj, hash)
        else
            match vgb.TryGetBindingByHash hash with
            | Some _ -> VagabondValue hash
            | None -> Cached hash

/// Cached value representation; can be stored as materialized object or pickled bytes
[<AutoSerializable(false); NoEquality; NoComparison>]
type private CachedValue = 
    | Pickled of byte []
    | Reified of obj
with
    member inline cv.Value = 
        match cv with
        | Pickled bytes -> VagabondRegistry.Instance.Serializer.UnPickle<obj> bytes
        | Reified o -> o

    static member inline Create(value : obj, pickle:bool) = 
        if pickle then
            let bytes = VagabondRegistry.Instance.Serializer.Pickle<obj> value
            Pickled bytes
        else
            Reified value

/// Header value serialized at the beginning of a disk-persisted value
[<NoEquality; NoComparison>]
type private CloudValueHeader =
    {
        Type : Type
        Hash : HashResult
        Level : StorageLevel
    }

/// StoreConfiguration object specific to local process
[<AutoSerializable(false); NoEquality; NoComparison>]
type private StoreCloudValueConfiguration =
    {
        /// Unique StoreCloudValue configuration id
        Id : string
        /// Encapsulation threshold for objects in bytes
        EncapsulatationTreshold : int64
        /// Cloud file store instance used for persisting values
        FileStore : ICloudFileStore
        /// Directory containing cached values in store
        StoreContainer : string
        /// Serializer instance used by store
        Serializer : ISerializer
        /// Local cache used by 
        LocalCache : InMemoryCache
    }
with
    /// <summary>
    ///     Creates a filename based on given hashcode
    /// </summary>
    /// <param name="hash">Input hash</param>
    member c.GetPath (hash : HashResult) =
        let truncate (n : int) (txt : string) =
            if n <= txt.Length then txt
            else txt.Substring(0, n)

        let base32Enc = Convert.BytesToBase32 (Array.append (BitConverter.GetBytes hash.Length) hash.Hash)
        let fileName = sprintf "%s-%s" (truncate 7 hash.Type) base32Enc
        c.FileStore.Combine(c.StoreContainer, fileName)

/// Registry for StoreCloudValue configurations; used for StoreCloudValue deserialization
type private StoreCloudValueRegistry private () =
    static let container = new ConcurrentDictionary<string, StoreCloudValueConfiguration> ()
    static member Register(config : StoreCloudValueConfiguration) = container.TryAdd(config.Id, config)
    static member TryGetById(id : string) = 
        let ok,value = container.TryGetValue id
        if ok then Some value
        else None

    static member RemoveById(id : string) = container.TryRemove id

/// Store-based CloudValue implementation
[<DataContract>]
type private StoreCloudValue<'T> internal (id:CachedEntityId, reflectedType : Type, level : StorageLevel, config : StoreCloudValueConfiguration) =

    [<DataMember(Name = "Id")>]
    let id = id

    [<DataMember(Name = "ReflectedType")>]
    let reflectedType = reflectedType

    [<DataMember(Name = "StorageLevel")>]
    let level = level

    [<DataMember(Name = "ConfigId")>]
    let configId = config.Id

    /// configuration object not serialized
    [<IgnoreDataMember>]
    let mutable config = Some config
    let getConfig() =
        match config with
        | None -> 
            let msg = sprintf "No StoreCloudValueConfiguration of id '%s' has been registered in the local context." configId
            invalidOp msg
        | Some c -> c

    /// asynchronously fetches/caches value from store.
    let getValue () = async {
        match id with
        | Encapsulated (value,_) -> return value :?> 'T
        | VagabondValue hash ->
            let f = VagabondRegistry.Instance.TryGetBindingByHash hash |> Option.get
            return f.GetValue(null) :?> 'T

        | Cached hash ->
            let config = getConfig()
            match config.LocalCache.TryFind hash.Id with
            | Some (:? CachedValue as cv) -> return cv.Value :?> 'T
            | Some _ -> return invalidOp "StoreCloudValue: internal error, cached value was of invalid type."
            | None when level.HasFlag StorageLevel.Disk ->
                let filePath = config.GetPath hash
                use! fs = config.FileStore.BeginRead filePath
                let _ = config.Serializer.Deserialize<CloudValueHeader>(fs, leaveOpen = true)
                let value = config.Serializer.Deserialize<obj>(fs, leaveOpen = true) :?> 'T
                return
                    if level.HasFlag StorageLevel.MemorySerialized then
                        ignore <| config.LocalCache.Add(hash.Id, CachedValue.Create(value, pickle = true))
                        value
                    elif level.HasFlag StorageLevel.Memory then
                        if config.LocalCache.Add(hash.Id, CachedValue.Create(value, pickle = false)) then value
                        else config.LocalCache.Get hash.Id :?> 'T
                    else
                        value

            | None ->
                return raise <| new ObjectDisposedException(sprintf "CloudValue '%s'not found." hash.Id)
    }

    /// StoreCloudValue factory method which ensures that array types are mapped to the proper ICloudArray subtype
    static member internal CreateReflected(id : CachedEntityId, containerType : Type, reflectedType : Type, level : StorageLevel, config : StoreCloudValueConfiguration) =
        if containerType.IsArray && containerType.GetArrayRank() = 1 then
            let et = containerType.GetElementType()
            let e = Existential.FromType et
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'et> () = new StoreCloudArray<'et>(id, level, config) :> ICloudValue }
        else
            let e = Existential.FromType containerType
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'T>() = new StoreCloudValue<'T>(id, reflectedType, level, config) :> ICloudValue }

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        config <- StoreCloudValueRegistry.TryGetById configId

    member internal __.Dispose() = async {
        match id with
        | Encapsulated _
        | VagabondValue _ -> return ()
        | Cached hash -> 
            let config = getConfig()
            do! config.FileStore.DeleteFile (config.GetPath hash)
            ignore <| config.LocalCache.Delete hash.Id
    }

    interface ICloudValue<'T> with
        member x.Dispose(): Async<unit> = async {
            return! x.Dispose()
        }
        
        member x.GetBoxedValueAsync(): Async<obj> = async {
            let! t = getValue ()
            return box t
        }
        
        member x.GetValueAsync(): Async<'T> = getValue()
        
        member x.Id: string = id.Hash.Id
        
        member x.IsCachedLocally: bool = 
            match id with
            | Encapsulated _ -> true
            | VagabondValue _ -> true
            | Cached hash -> getConfig().LocalCache.ContainsKey hash.Id
        
        member x.Size: int64 = id.Hash.Length
        
        member x.StorageLevel: StorageLevel = level
        
        member x.Type: Type = typeof<'T>
        
        member x.Value: 'T =
            getValue() |> Async.RunSync
        
        member x.GetBoxedValue() : obj = 
            getValue() |> Async.RunSync |> box

        member x.Cast<'S> () : ICloudValue<'S> =
            if typeof<'S>.IsAssignableFrom reflectedType then
                StoreCloudValue<_>.CreateReflected(id, typeof<'S>, reflectedType, level, getConfig()) :?> ICloudValue<'S>
            else
                raise <| new InvalidCastException()
            

and [<Sealed; DataContract>]
  private StoreCloudArray<'T>(id:CachedEntityId, level : StorageLevel, config : StoreCloudValueConfiguration) =
    inherit StoreCloudValue<'T []>(id, typeof<'T[]>, level, config)

    interface ICloudArray<'T> with
        member x.Length = (x :> ICloudValue<'T []>).Value.Length

    interface seq<'T> with
        member x.GetEnumerator() = (x :> ICloudValue<'T []>).Value.GetEnumerator()
        member x.GetEnumerator() = ((x :> ICloudValue<'T []>).Value :> seq<'T>).GetEnumerator()

    interface ICloudCollection<'T> with
        member x.GetCount(): Async<int64> = async {
            let! v = (x :> ICloudValue<'T []>).GetValueAsync()
            return int64 v.Length
        }
        
        member x.GetSize(): Async<int64> = async {
            return (x :> ICloudValue).Size
        }
        
        member x.IsKnownCount: bool = (x :> ICloudValue).IsCachedLocally
        
        member x.IsKnownSize: bool = true
        
        member x.IsMaterialized: bool = (x :> ICloudValue).IsCachedLocally
        
        member x.ToEnumerable(): Async<seq<'T>> = async {
            let! v = (x :> ICloudValue<'T []>).GetValueAsync()
            return v :> seq<'T>
        }

/// CloudValue provider implementation that is based on cloud storage
and [<AutoSerializable(false)>]
  StoreCloudValueProvider private (config : StoreCloudValueConfiguration) =
    let mutable isDisposed = false
    let ensureActive() =
        if isDisposed then
            raise <| new ObjectDisposedException("StoreCloudValueProvider has been disposed.")

    let partitionBySize (threshold:int64) (ts:seq<'T>) =
        let accumulated = new ResizeArray<'T []>()
        let array = new ResizeArray<'T> ()
        // avoid Option<Pickler<_>> allocations in every iteration by creating it here
        let pickler = FsPickler.GeneratePickler<'T>() |> Some
        let mutable sizeCounter = FsPickler.CreateSizeCounter()
        use enum = ts.GetEnumerator()
        while enum.MoveNext() do
            let t = enum.Current
            array.Add t
            sizeCounter.Append(t, ?pickler = pickler)
            if sizeCounter.Count > threshold then
                accumulated.Add(array.ToArray())
                array.Clear()
                sizeCounter <- FsPickler.CreateSizeCounter()

        if array.Count > 0 then
            accumulated.Add(array.ToArray())
            array.Clear()

        accumulated :> seq<'T []>

    let getPersistedCloudValueByPath(path : string) = async {
        use! stream = config.FileStore.BeginRead path
        try
            let header = config.Serializer.Deserialize<CloudValueHeader>(stream, leaveOpen = true)
            let ceid = Cached header.Hash
            return StoreCloudArray<_>.CreateReflected(ceid, header.Type, header.Type, header.Level, config)
        with e ->
            return raise <| new FormatException(sprintf "Could not read CloudValue from '%s'." path, e)
    }

    let persistValue (level : StorageLevel) (hash : HashResult) (value : obj) = async {
        let path = config.GetPath hash
        let! exists = config.FileStore.FileExists path
        if not exists then
            use! stream = config.FileStore.BeginWrite path
            let header = { Type = getReflectedType value ; Hash = hash ; Level = level }
            config.Serializer.Serialize<CloudValueHeader>(stream, header, leaveOpen = true)
            config.Serializer.Serialize<obj>(stream, value, leaveOpen = true)
    }

    let createCloudValue (level : StorageLevel) (value:'T) = async {
        let ceid = CachedEntityId.FromObject(value, config.EncapsulatationTreshold)

        match ceid with
        | Cached hash ->
            // persist to store; TODO: make asynchronous?
            if level.HasFlag StorageLevel.Disk then do! persistValue level hash value

            // persist to cache
            if level.HasFlag StorageLevel.MemorySerialized then
                ignore <| config.LocalCache.Add(hash.Id, CachedValue.Create(value, pickle = true))
            elif level.HasFlag StorageLevel.Memory then
                ignore <| config.LocalCache.Add(hash.Id, CachedValue.Create(value, pickle = false))
                
        | _ -> ()

        return StoreCloudValue<_>.CreateReflected(ceid, typeof<'T>, getReflectedType value, level, config) :?> ICloudValue<'T>
    }

    /// <summary>
    ///     Initializes a CloudValueProvider instance that is based on a cloud storage service.
    /// </summary>
    /// <param name="fileStore">Cloud file store instance used for persisting values.</param>
    /// <param name="storeContainer">Container directory used for persisting values.</param>
    /// <param name="serializer">Serializer instance used for persisting values. Defaults to FsPickler.</param>
    /// <param name="encapsulationThreshold">Values less than this size will be encapsulated in CloudValue instance. Defaults to 5KiB.</param>
    static member InitCloudValueProvider(fileStore:ICloudFileStore, storeContainer:string, ?serializer:ISerializer, ?encapsulationThreshold:int64) =
        let id = sprintf "%s:%s/%s" fileStore.Name fileStore.Id storeContainer
        let serializer = match serializer with Some s -> s | None -> new FsPicklerBinaryStoreSerializer() :> ISerializer
        let encapsulationThreshold = defaultArg encapsulationThreshold (5L * 1024L)
        let cache = InMemoryCache.Create()
        let config = 
            { 
                Id = id
                FileStore = fileStore
                StoreContainer = storeContainer
                Serializer = serializer
                LocalCache = cache
                EncapsulatationTreshold = encapsulationThreshold
            }

        if not <| StoreCloudValueRegistry.Register config then
            invalidOp <| sprintf "A CloudValue configuration of id '%s' has already been registered." id

        new StoreCloudValueProvider(config)

    /// Disposes particular StoreCloudValueInstance instance. 
    member __.UnRegister() = 
        if not isDisposed then 
            ignore <| StoreCloudValueRegistry.RemoveById config.Id
            isDisposed <- true

    interface ICloudValueProvider with
        member x.Id: string = sprintf "StoreCloudValue [%s] at %s." config.FileStore.Id config.StoreContainer
        member x.Name: string = "StoreCloudValue"
        member x.DefaultStorageLevel = StorageLevel.MemoryAndDisk
        member x.IsSupportedStorageLevel (_ : StorageLevel) = true

        member x.CreatePartitionedArray(values : seq<'T>, level : StorageLevel, ?partitionThreshold : int64) = async {
            match partitionThreshold with
            | None -> let! cv = createCloudValue level (Seq.toArray values) in return [| cv :?> ICloudArray<'T> |]
            | Some pt -> 
                return!
                    values
                    |> partitionBySize pt
                    |> Seq.map (fun vs -> async { let! cv = createCloudValue level vs in return cv :?> ICloudArray<'T> })
                    |> Async.Parallel
        }

        member x.CreateCloudValue(payload: 'T, level : StorageLevel): Async<ICloudValue<'T>> = async {
            ensureActive()
            return! createCloudValue level payload
        }
        
        member x.Dispose(value: ICloudValue): Async<unit> = async {
            ensureActive()
            return! value.Dispose()
        }
        
        member x.DisposeAllValues(): Async<unit> = async {
            ensureActive()
            let! files = config.FileStore.EnumerateFiles(config.StoreContainer)
            return!
                files
                |> Seq.map config.FileStore.DeleteFile
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member x.GetAllValues(): Async<ICloudValue []> = async {
            ensureActive()
            let! files = config.FileStore.EnumerateFiles(config.StoreContainer)
            let! cvalues =
                files
                |> Seq.map (getPersistedCloudValueByPath >> Async.Catch)
                |> Async.Parallel

            return cvalues |> Array.choose (function Choice1Of2 v -> Some v | _ -> None)
        }

        member x.GetById(id:string) : Async<ICloudValue> = async {
            ensureActive()
            let path = config.FileStore.Combine(config.StoreContainer, id)
            return! getPersistedCloudValueByPath path
        }