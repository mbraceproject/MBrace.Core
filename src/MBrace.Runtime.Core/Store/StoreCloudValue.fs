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

[<AutoOpen>]
module private StoreCloudValueConfig =
    
    [<Literal>]
    let persistFileSuffix = ".cv"

// StoreCloudValue instances have three possible representations
//  * Encapsulated: values small enough to be encapsulated in the CloudValue instance
//  * VagabondValue: large values managed by Vagabond and are therefore available in the local context
//  * Cached: values that are cached by the implementation
[<AutoSerializable(true); NoEquality; NoComparison>]
type private CachedEntityId =
    | Encapsulated of value:obj * hash:HashResult
    | VagabondValue of hash:HashResult
    | Cached of level:StorageLevel * hash:HashResult
with
    /// Gets the FsPickler hashcode of the cached value
    member c.Hash =
        match c with
        | Encapsulated (_,hash) -> hash
        | Cached (_, hash) -> hash
        | VagabondValue hash -> hash

    member c.Level =
        match c with
        | Encapsulated _ -> StorageLevel.Encapsulated
        | VagabondValue _ -> StorageLevel.Other
        | Cached (l,_) -> l

    /// <summary>
    ///     Creates a cache entity identifier for provided object graph.
    /// </summary>
    /// <param name="obj">Input serializable object graph.</param>
    /// <param name="sizeThreshold">Size threshold in bytes. Objects less than the treshold will be encapsulated in CloudValue instance.</param>
    static member FromObject(obj:obj, level : StorageLevel, sizeThreshold:int64) : CachedEntityId =
        let vgb = VagabondRegistry.Instance
        let hash = vgb.Serializer.ComputeHash obj
        if obj = null || hash.Length <= sizeThreshold then Encapsulated(obj, hash)
        else
            match vgb.TryGetBindingByHash hash with
            | Some _ -> VagabondValue hash
            | None -> Cached(level, hash)

/// Cached value representation; can be stored as materialized object or pickled bytes
[<AutoSerializable(false); NoEquality; NoComparison>]
type private CachedValue = 
    | Pickled of StorageLevel * byte []
    | Reified of StorageLevel * obj
with
    member inline cv.Level =
        match cv with
        | Pickled (l,_) -> l
        | Reified (l,_) -> l

    member inline cv.Value = 
        match cv with
        | Pickled (_, bytes) -> VagabondRegistry.Instance.Serializer.UnPickle<obj> bytes
        | Reified (_, o) -> o

    static member inline Create(value : obj, level : StorageLevel) = 
        if level.HasFlag StorageLevel.MemorySerialized then
            let bytes = VagabondRegistry.Instance.Serializer.Pickle<obj> value
            Pickled(level, bytes)
        else
            Reified(level, value)

/// Header value serialized at the beginning of a disk-persisted value
[<NoEquality; NoComparison>]
type private CloudValueHeader =
    {
        /// Reflected type of persisted value
        Type : Type
        /// FsPickler hashcode for persisted value
        Hash : HashResult
        /// User-specified storage level for entity
        Level : StorageLevel
        /// Element count if persisted array
        Count : int
    }

/// StoreConfiguration object specific to local process
[<AutoSerializable(true); NoEquality; NoComparison>]
type private StoreCloudValueConfiguration =
    {
        /// Unique StoreCloudValue configuration id
        Id : string
        /// Encapsulation threshold for objects in bytes
        EncapsulationTreshold : int64
        /// Cloud file store instance used for persisting values
        FileStore : ICloudFileStore
        /// Directory containing cached values in store
        StoreContainer : string
        /// Serializer instance used by store
        Serializer : ISerializer
        /// Cache factory method
        CacheFactory : (unit -> InMemoryCache) option
    }
with
    /// <summary>
    ///     Creates a filename based on given hashcode
    /// </summary>
    /// <param name="hash">Input hash</param>
    member c.GetPath (hash : HashResult) =
        let truncate (n : int) (txt : string) =
            if n >= txt.Length then txt
            else txt.Substring(0, n)

        let base32Enc = Convert.BytesToBase32 (Array.append (BitConverter.GetBytes hash.Length) hash.Hash)
        let fileName = sprintf "%s-%s.%s" (truncate 40 hash.Type) base32Enc persistFileSuffix
        c.FileStore.Combine(c.StoreContainer, fileName)

/// Registry for StoreCloudValue configurations; used for StoreCloudValue deserialization
type private StoreCloudValueRegistry private () =
    static let container = new ConcurrentDictionary<string, InMemoryCache * StoreCloudValueConfiguration> ()
    static member Install(config : StoreCloudValueConfiguration) = 
        let mkLocalCache () =
            match config.CacheFactory with
            | None -> InMemoryCache.Create()
            | Some cf -> cf ()

        container.GetOrAdd(config.Id, fun _ -> (mkLocalCache (), config))

    static member Uninstall(id : string) = 
        let mutable v = Unchecked.defaultof<_>
        container.TryRemove(id, &v)

    static member TryGetConfiguration(id : string) =
        let mutable v = Unchecked.defaultof<_>
        if container.TryGetValue(id, &v) then Some v
        else None

/// Store-based CloudValue implementation
[<DataContract>]
type private StoreCloudValue<'T> internal (id:CachedEntityId, elementCount:int, reflectedType : Type, config : StoreCloudValueConfiguration, localCache : InMemoryCache) =

    [<DataMember(Name = "Id")>]
    let id = id

    [<DataMember(Name = "Count")>]
    let elementCount = elementCount

    [<DataMember(Name = "ReflectedType")>]
    let reflectedType = reflectedType

    [<DataMember(Name = "ConfigId")>]
    let configId = config.Id

    /// configuration object not serialized
    [<IgnoreDataMember>]
    let mutable config = Some config
    [<IgnoreDataMember>]
    let mutable localCache = localCache

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

        | Cached (level, hash) ->
            let config = getConfig()
            match localCache.TryFind hash.Id with
            | Some (:? CachedValue as cv) -> return cv.Value :?> 'T
            | Some _ -> return invalidOp "StoreCloudValue: internal error, cached value was of invalid type."
            | None when level.HasFlag StorageLevel.Disk ->
                let filePath = config.GetPath hash
                try
                    use! stream = config.FileStore.BeginRead filePath
                    let _ = config.Serializer.Deserialize<CloudValueHeader>(stream, leaveOpen = true)
                    let value = config.Serializer.Deserialize<obj>(stream, leaveOpen = true) :?> 'T
                    return
                        if level.HasFlag StorageLevel.MemorySerialized then
                            ignore <| localCache.Add(hash.Id, CachedValue.Create(value, level))
                            value
                        elif level.HasFlag StorageLevel.Memory then
                            let isSuccess = localCache.Add(hash.Id, CachedValue.Create(value, level))
                            if isSuccess then value
                            else localCache.Get hash.Id :?> 'T
                        else
                            value

                with 
                | :? System.IO.FileNotFoundException -> 
                    return raise <| new ObjectDisposedException(hash.Id, "Could not dereference CloudValue from store.")

                | :? System.Runtime.Serialization.SerializationException
                | :? Nessos.FsPickler.FsPicklerException as e ->
                    return raise <| new SerializationException(sprintf "Error deserializing CloudValue '%s'" hash.Id, e)

            | None ->
                return raise <| new ObjectDisposedException(hash.Id, "Could not dereference CloudValue from local cache.")
    }

    member internal __.ElementCount = elementCount

    /// StoreCloudValue factory method which ensures that array types are mapped to the proper ICloudArray subtype
    static member internal CreateReflected(id : CachedEntityId, elementCount:int, containerType : Type, reflectedType : Type, config : StoreCloudValueConfiguration, localCache : InMemoryCache) =
        if containerType.IsArray && containerType.GetArrayRank() = 1 then
            let et = containerType.GetElementType()
            let e = Existential.FromType et
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'et> () = new StoreCloudArray<'et>(id, elementCount, config, localCache) :> ICloudValue }
        else
            let e = Existential.FromType containerType
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'T>() = new StoreCloudValue<'T>(id, elementCount, reflectedType, config, localCache) :> ICloudValue }

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        match StoreCloudValueRegistry.TryGetConfiguration configId with
        | Some(lc,cfg) -> localCache <- lc ; config <- Some cfg
        | None -> ()

    interface ICloudValue<'T> with
        member x.Dispose(): Async<unit> = async {
            match id with
            | Encapsulated _
            | VagabondValue _ -> return ()
            | Cached (level, hash) -> 
                let config = getConfig()
                if level.HasFlag StorageLevel.Disk then do! config.FileStore.DeleteFile (config.GetPath hash)
                if level.HasFlag StorageLevel.Memory || level.HasFlag StorageLevel.MemorySerialized then
                    ignore <| localCache.Delete hash.Id
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
            | Cached (_,hash) -> localCache.ContainsKey hash.Id
        
        member x.Size: int64 = id.Hash.Length
        
        member x.StorageLevel: StorageLevel = id.Level
        
        member x.Type: Type = typeof<'T>

        member x.ReflectedType : Type = reflectedType
        
        member x.Value: 'T =
            getValue() |> Async.RunSync
        
        member x.GetBoxedValue() : obj = 
            getValue() |> Async.RunSync |> box

        member x.Cast<'S> () : ICloudValue<'S> =
            if typeof<'S>.IsAssignableFrom reflectedType then
                StoreCloudValue<_>.CreateReflected(id, elementCount, typeof<'S>, reflectedType, getConfig(), localCache) :?> ICloudValue<'S>
            else
                raise <| new InvalidCastException()
            

and [<Sealed; DataContract>]
  private StoreCloudArray<'T>(id:CachedEntityId, elementCount:int, config : StoreCloudValueConfiguration, localCache : InMemoryCache) =
    inherit StoreCloudValue<'T []>(id, elementCount, typeof<'T[]>, config, localCache)

    interface ICloudArray<'T> with
        member x.Length = x.ElementCount

    interface seq<'T> with
        member x.GetEnumerator() = (x :> ICloudValue<'T []>).Value.GetEnumerator()
        member x.GetEnumerator() = ((x :> ICloudValue<'T []>).Value :> seq<'T>).GetEnumerator()

    interface ICloudCollection<'T> with
        member x.GetCount(): Async<int64> = async {
            return int64 x.ElementCount
        }
        
        member x.GetSize(): Async<int64> = async {
            return (x :> ICloudValue).Size
        }
        
        member x.IsKnownCount: bool = true
        member x.IsKnownSize: bool = true
        member x.IsMaterialized: bool = (x :> ICloudValue).IsCachedLocally
        
        member x.ToEnumerable(): Async<seq<'T>> = async {
            let! v = (x :> ICloudValue<'T []>).GetValueAsync()
            return v :> seq<'T>
        }

/// CloudValue provider implementation that is based on cloud storage
and [<Sealed; DataContract>] StoreCloudValueProvider private (config : StoreCloudValueConfiguration, localCache : InMemoryCache) =

    static let isSupportedLevel (level : StorageLevel) =
        level.HasFlag StorageLevel.Disk || 
        level.HasFlag StorageLevel.Memory || 
        level.HasFlag StorageLevel.MemorySerialized

    [<DataMember(Name = "Configuration")>]
    let config = config

    [<IgnoreDataMember>]
    let mutable localCache = Some localCache
    let getLocalCache() =
        match localCache with
        | Some lc -> lc
        | None -> invalidOp <| sprintf "StoreCloudValue '%s' not installed in the local context." config.Id

    let ensureActive() =
        if Option.isNone localCache then
            invalidOp <| sprintf "StoreCloudValue '%s' not installed in the local context." config.Id

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
            let ceid = Cached(header.Level, header.Hash)
            return StoreCloudArray<_>.CreateReflected(ceid, header.Count, header.Type, header.Type, config, getLocalCache())
        with e ->
            return raise <| new FormatException(sprintf "Could not read CloudValue from '%s'." path, e)
    }

    let persistValue (level : StorageLevel) (hash : HashResult) (elementCount : int) (value : obj) = async {
        let path = config.GetPath hash
        let! exists = config.FileStore.FileExists path
        if not exists then
            use! stream = config.FileStore.BeginWrite path
            let header = { Type = getReflectedType value ; Hash = hash ; Level = level ; Count = elementCount }
            config.Serializer.Serialize<CloudValueHeader>(stream, header, leaveOpen = true)
            config.Serializer.Serialize<obj>(stream, value, leaveOpen = true)
    }

    let createCloudValue (level : StorageLevel) (value:'T) = async {
        let ceid = CachedEntityId.FromObject(value, level, config.EncapsulationTreshold)
        let elementCount = match box value with :? System.Array as a when a.Rank = 1 -> a.Length | _ -> 1

        match ceid with
        | Cached (level, hash) ->
            // persist to store; TODO: make asynchronous?
            if level.HasFlag StorageLevel.Disk then do! persistValue level hash elementCount value

            // persist to cache
            if level.HasFlag StorageLevel.MemorySerialized || level.HasFlag StorageLevel.Memory then
                ignore <| getLocalCache().Add(hash.Id, CachedValue.Create(value, level))
                
        | _ -> ()

        return StoreCloudValue<_>.CreateReflected(ceid, elementCount, typeof<'T>, getReflectedType value, config, getLocalCache()) :?> ICloudValue<'T>
    }

    /// <summary>
    ///     Initializes a CloudValueProvider instance that is based on a cloud storage service.
    /// </summary>
    /// <param name="fileStore">Cloud file store instance used for persisting values.</param>
    /// <param name="storeContainer">Container directory used for persisting values.</param>
    /// <param name="cacheFactory">User-supplied InMemory cache factory. Must be serializable.</param>
    /// <param name="serializer">Serializer instance used for persisting values. Defaults to FsPickler.</param>
    /// <param name="encapsulationThreshold">Values less than this size will be encapsulated in CloudValue instance. Defaults to 512KiB.</param>
    static member InitCloudValueProvider(fileStore:ICloudFileStore, storeContainer:string, ?cacheFactory : unit -> InMemoryCache, ?serializer:ISerializer, ?encapsulationThreshold:int64) =
        let id = sprintf "%s:%s/%s" fileStore.Name fileStore.Id storeContainer
        let serializer = match serializer with Some s -> s | None -> new FsPicklerBinaryStoreSerializer() :> ISerializer
        let encapsulationThreshold = defaultArg encapsulationThreshold (512L * 1024L)
        FsPickler.EnsureSerializable cacheFactory
        let config = 
            { 
                Id = id
                FileStore = fileStore
                StoreContainer = storeContainer
                Serializer = serializer
                CacheFactory = cacheFactory
                EncapsulationTreshold = encapsulationThreshold
            }

        let localCache, config' = StoreCloudValueRegistry.Install config
        new StoreCloudValueProvider(config', localCache)

    [<OnDeserialized>]
    member private __.OnDeserialized(_ : StreamingContext) =
        match StoreCloudValueRegistry.TryGetConfiguration(config.Id) with
        | Some(lc,_) -> localCache <- Some lc
        | None -> ()

    /// Installs InMemory cache for StoreCloudValue instance in the local context
    member __.Install() =
        lock config (fun () ->
            match localCache with
            | Some _ -> ()
            | None ->
                let lc, _ = StoreCloudValueRegistry.Install(config)
                localCache <- Some lc)

    /// Uninstalls InMemory cache for StoreCloudValue instance in the local context
    member __.Uninstall () =
        lock config (fun () ->
            match localCache with
            | None -> ()
            | Some _ ->
                ignore <| StoreCloudValueRegistry.Uninstall(config.Id))

    interface ICloudValueProvider with
        member x.Id: string = sprintf "StoreCloudValue [%s] at %s." config.FileStore.Id config.StoreContainer
        member x.Name: string = "StoreCloudValue"
        member x.DefaultStorageLevel = StorageLevel.MemoryAndDisk
        member x.IsSupportedStorageLevel (level : StorageLevel) = isSupportedLevel level

        member x.CreatePartitionedArray(values : seq<'T>, level : StorageLevel, ?partitionThreshold : int64) = async {
            ensureActive()
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Not supported storage level '%O'." level
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
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Not supported storage level '%O'." level
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
                |> Seq.filter (fun f -> f.EndsWith persistFileSuffix)
                |> Seq.map config.FileStore.DeleteFile
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member x.GetAllValues(): Async<ICloudValue []> = async {
            ensureActive()
            let! files = config.FileStore.EnumerateFiles(config.StoreContainer)
            let! cvalues =
                files
                |> Seq.filter (fun f -> f.EndsWith persistFileSuffix)
                |> Seq.map (getPersistedCloudValueByPath >> Async.Catch)
                |> Async.Parallel

            return cvalues |> Array.choose (function Choice1Of2 v -> Some v | _ -> None)
        }

        member x.GetValueById(id:string) : Async<ICloudValue> = async {
            ensureActive()
            let hash = HashResult.Parse id
            let localCache = getLocalCache()
            match getLocalCache().TryFind hash.Id with
            // Step 1. attempt to resolve CloudValue from local cache state
            | Some (:? CachedValue as cv) when not <| let l = cv.Level in not <| l.HasFlag StorageLevel.Disk ->
                let value = cv.Value
                let count = match value with :? System.Array as a when a.Rank = 1 -> a.Length | _ -> 1
                let reflectedType = getReflectedType value
                return StoreCloudValue<_>.CreateReflected(Cached (cv.Level, hash), count, reflectedType, reflectedType, config, localCache)

            // Step 2. attempt to look up from store
            | _ ->
                let path = config.GetPath hash
                try return! getPersistedCloudValueByPath path
                with :? System.IO.FileNotFoundException ->
                    return raise <| new ObjectDisposedException(hash.Id, "CloudValue could not be found in store.")
        }