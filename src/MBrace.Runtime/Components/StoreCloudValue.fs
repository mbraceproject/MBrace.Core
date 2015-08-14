namespace MBrace.Runtime.Components

open System
open System.Reflection
open System.IO
open System.Collections.Concurrent
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.FsPickler.Hashing
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.PrettyPrinters

[<AutoOpen>]
module private StoreCloudValueImpl =
    
    [<Literal>]
    let persistFileSuffix = ".cv"

    // StoreCloudValue instances have three possible representations
    //  * Encapsulated: values small enough to be encapsulated in the CloudValue instance
    //  * VagabondValue: large values managed by Vagabond and are therefore available in the local context
    //  * Cached: values that are cached by the implementation
    [<AutoSerializable(true); NoEquality; NoComparison>]
    type CachedEntityId =
        | Encapsulated of value:obj * hash:HashResult
        | VagabondValue of field:FieldInfo * hash:HashResult
        | Cached of level:StorageLevel * hash:HashResult
    with
        /// Gets the FsPickler hashcode of the cached value
        member c.Hash =
            match c with
            | Encapsulated (_,hash) -> hash
            | Cached (_, hash) -> hash
            | VagabondValue (_, hash) -> hash

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
                | Some f -> VagabondValue(f, hash)
                | None -> Cached(level, hash)

    /// Cached value representation; can be stored as materialized object or pickled bytes
    [<AutoSerializable(false); NoEquality; NoComparison>]
    type CachedValue = 
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
    type CloudValueHeader =
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
    with
        static member FromValue(value : 'T, hash : HashResult, level : StorageLevel) =
            {
                Type = getReflectedType value
                Hash = hash
                Level = level
                Count = match box value with :? System.Array as a when a.Rank = 1 -> a.Length | _ -> 1
            }

    /// Global StoreConfiguration object
    [<AutoSerializable(true); NoEquality; NoComparison>]
    type StoreCloudValueConfiguration =
        {
            /// Unique StoreCloudValue configuration id
            Id : string
            /// Encapsulation threshold for objects in bytes
            EncapsulationTreshold : int64
            /// Global cloud file store location used for persisting values
            MainStore : CloudFileStoreConfiguration
            /// File store instance used by local instance
            LocalStore : (unit -> CloudFileStoreConfiguration) option
            /// Serializer instance used by store
            Serializer : ISerializer
            /// Cache factory method
            CacheFactory : (unit -> InMemoryCache) option
            /// Asynchronously persist 'StorageLevel.Memory' to disk
            ShadowPersistObjects : bool
        }

    /// StoreConfiguration object specific to local process
    [<AutoSerializable(false); NoEquality; NoComparison>]
    type LocalStoreCloudValueConfiguration =
        {
            /// Global, serializable, configuration object
            Global : StoreCloudValueConfiguration
            /// Local CloudFileStore implementation
            LocalStore : CloudFileStoreConfiguration option
            /// Local in-memory cache
            LocalCache : InMemoryCache
        }
    with
        static member Init(config : StoreCloudValueConfiguration) =
            {
                Global = config
                LocalStore = config.LocalStore |> Option.map (fun f -> f ())
                LocalCache =   
                    match config.CacheFactory with
                    | None -> InMemoryCache.Create(name = config.Id, physicalMemoryLimitPercentage = 80, pollingInterval = TimeSpan.FromSeconds 0.5)
                    | Some cf -> cf ()
            }

    type CloudFileStoreConfiguration with

        /// <summary>
        ///     Creates a filename based on given hashcode.
        /// </summary>
        /// <param name="hash">Input hash.</param>
        member c.GetPathByHash (hash : HashResult) =
            let fileName = Vagabond.GetUniqueFileName hash
            c.FileStore.Combine(c.DefaultDirectory, fileName + persistFileSuffix)

        /// <summary>
        ///     Checks if object by given hash is persisted in store.
        /// </summary>
        /// <param name="hash">Hash to be checked.</param>
        member c.ContainsHash(hash : HashResult) = async {
            let filePath = c.GetPathByHash hash
            return! c.FileStore.FileExists filePath
        }

        /// <summary>
        ///     Deletes persisted object of given hash from underlying store.
        /// </summary>
        /// <param name="hash">Hash to be deleted.</param>
        member c.DeleteByHash(hash : HashResult) = async {
            let filePath = c.GetPathByHash hash
            do! c.FileStore.DeleteFile filePath
        }

        /// <summary>
        ///     Persists an object with provided header to underlying store.
        /// </summary>
        /// <param name="serializer">Serializer used for persistence.</param>
        /// <param name="header">CloudValue header object.</param>
        /// <param name="value">Value to be persisted.</param>
        /// <param name="overwrite">Overwrite persisted file if it already exists.</param>
        member c.Serialize<'T>(serializer : ISerializer, header : CloudValueHeader, value : 'T, overwrite : bool) = async {
            let filePath = c.GetPathByHash header.Hash
            let! exists = c.FileStore.FileExists filePath
            if not exists || overwrite then
                use! stream = c.FileStore.BeginWrite filePath
                serializer.Serialize<CloudValueHeader>(stream, header, leaveOpen = true)
                serializer.Serialize<obj>(stream, value, leaveOpen = true)
        }

        /// <summary>
        ///     Attempts reading cloud value header from underlying store.
        /// </summary>
        /// <param name="serializer">Serializer to be used.</param>
        /// <param name="path">Path to file to be read.</param>
        member c.TryGetHeader(serializer : ISerializer, path : string) = async {
            try
                use! stream = c.FileStore.BeginRead path
                let header = serializer.Deserialize<CloudValueHeader>(stream, leaveOpen = true)
                return Some header
            with 
            | :? System.IO.FileNotFoundException -> return None
            | e -> return raise <| new System.IO.InvalidDataException(sprintf "Error deserializing CloudValue header at '%s'." path, e)
        }

        /// <summary>
        ///     Attempts deserializing persisted cloud value by hash.
        /// </summary>
        /// <param name="serializer">Serializer to be used.</param>
        /// <param name="hash">Hash identifier for object.</param>
        member c.TryDeserialize<'T>(serializer : ISerializer, hash : HashResult) = async {
            let filePath = c.GetPathByHash hash
            try
                use! stream = c.FileStore.BeginRead filePath
                let _ = serializer.Deserialize<CloudValueHeader>(stream, leaveOpen = true)
                let value = serializer.Deserialize<obj>(stream, leaveOpen = true) :?> 'T
                return Some value

            with 
            | :? System.IO.FileNotFoundException -> return None
            | e -> return raise <| new System.IO.InvalidDataException(sprintf "Error deserializing CloudValue at '%s'." filePath, e)
        }




/// Registry for StoreCloudValue configurations; used for StoreCloudValue deserialization
type private StoreCloudValueRegistry private () =
    static let container = new ConcurrentDictionary<string, LocalStoreCloudValueConfiguration> ()
    static member Install(config : StoreCloudValueConfiguration) =
        container.GetOrAdd(config.Id, fun _ -> LocalStoreCloudValueConfiguration.Init config)

    static member Uninstall(id : string) = 
        let mutable v = Unchecked.defaultof<_>
        container.TryRemove(id, &v)

    static member TryGetConfiguration(id : string) =
        let mutable v = Unchecked.defaultof<_>
        if container.TryGetValue(id, &v) then Some v
        else None

/// Store-based CloudValue implementation
[<DataContract>]
type private StoreCloudValue<'T> internal (id:CachedEntityId, elementCount:int, reflectedType : Type, config : LocalStoreCloudValueConfiguration) =

    [<DataMember(Name = "Id")>]
    let id = id

    [<DataMember(Name = "Count")>]
    let elementCount = elementCount

    [<DataMember(Name = "ReflectedType")>]
    let reflectedType = reflectedType

    [<DataMember(Name = "ConfigId")>]
    let configId = config.Global.Id

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
        | VagabondValue (f, _) -> return f.GetValue(null) :?> 'T
        | Cached (level, hash) ->
            let config = getConfig()
            // look up InMemory cache first
            match config.LocalCache.TryFind hash.Id with
            | Some (:? CachedValue as cv) -> return cv.Value :?> 'T
            | Some _ -> return invalidOp "StoreCloudValue: internal error, cached value was of invalid type."
            | None when not (level.HasFlag StorageLevel.Disk || config.Global.ShadowPersistObjects) -> 
                return raise <| new ObjectDisposedException(hash.Id, "Could not dereference CloudValue from local cache.")

            | None ->

                // look up local file system store cache, if available
                let! localCached = async {
                    match config.LocalStore with
                    | Some ls -> 
                        try return! ls.TryDeserialize<'T>(config.Global.Serializer, hash)
                        with _ -> return None
                    | _ -> return None
                }

                // look up global cache
                let! value = async {
                    match localCached with
                    | None -> 
                        let! globalValue = config.Global.MainStore.TryDeserialize<'T>(config.Global.Serializer, hash)
                        match globalValue with
                        // value not found in any storage level
                        | None -> return raise <| new ObjectDisposedException(hash.Id, "Could not dereference CloudValue from store.")
                        | Some gv ->
                            // serialize asynchronously to local store
                            match config.LocalStore with
                            | Some ls -> 
                                let header = CloudValueHeader.FromValue(gv, hash, level)
                                let _ = Async.StartAsTask(ls.Serialize<'T>(config.Global.Serializer, header, gv, overwrite = true))
                                ()

                            | _ -> ()

                        return globalValue

                    | lc -> return lc
                }

                match value with
                | None -> return raise <| new ObjectDisposedException(hash.Id, "Could not dereference CloudValue from store.")
                | Some value ->
                    // have value, cache in-memory before returning
                    return
                        if level.HasFlag StorageLevel.MemorySerialized then
                            ignore <| config.LocalCache.Add(hash.Id, CachedValue.Create(value, level))
                            value
                        elif level.HasFlag StorageLevel.Memory then
                            let isSuccess = config.LocalCache.Add(hash.Id, CachedValue.Create(value, level))
                            // if already cached, return cached value rather than downloaded value
                            if isSuccess then value
                            else 
                                let cv = config.LocalCache.Get(hash.Id) :?> CachedValue
                                cv.Value :?> 'T
                        else
                            value
    }

    member internal __.ElementCount = elementCount

    /// StoreCloudValue factory method which ensures that array types are mapped to the proper ICloudArray subtype
    static member internal CreateReflected(id : CachedEntityId, elementCount:int, containerType : Type, reflectedType : Type, config : LocalStoreCloudValueConfiguration) =
        if containerType.IsArray && containerType.GetArrayRank() = 1 then
            let et = containerType.GetElementType()
            let e = Existential.FromType et
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'et> () = new StoreCloudArray<'et>(id, elementCount, config) :> ICloudValue }
        else
            let e = Existential.FromType containerType
            e.Apply { new IFunc<ICloudValue> with member __.Invoke<'T>() = new StoreCloudValue<'T>(id, elementCount, reflectedType, config) :> ICloudValue }

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        match StoreCloudValueRegistry.TryGetConfiguration configId with
        | Some cfg -> config <- Some cfg
        | None -> ()

    interface CloudValue<'T> with
        member x.Dispose(): Async<unit> = async {
            match id with
            | Encapsulated _
            | VagabondValue _ -> return ()
            | Cached (level, hash) -> 
                let config = getConfig()
                if level.HasFlag StorageLevel.Disk then 
                    do! config.Global.MainStore.DeleteByHash hash
                    match config.LocalStore with
                    | Some ls -> do! ls.DeleteByHash hash
                    | None -> ()

                if level.HasFlag StorageLevel.Memory || level.HasFlag StorageLevel.MemorySerialized then
                    ignore <| config.LocalCache.Delete hash.Id
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
            | Cached (_,hash) -> getConfig().LocalCache.ContainsKey hash.Id
        
        member x.Size: int64 = id.Hash.Length
        
        member x.StorageLevel: StorageLevel = id.Level
        
        member x.Type: Type = typeof<'T>

        member x.ReflectedType : Type = reflectedType
        
        member x.Value: 'T =
            getValue() |> Async.RunSync
        
        member x.GetBoxedValue() : obj = 
            getValue() |> Async.RunSync |> box

        member x.Cast<'S> () : CloudValue<'S> =
            if typeof<'S>.IsAssignableFrom reflectedType then
                StoreCloudValue<_>.CreateReflected(id, elementCount, typeof<'S>, reflectedType, getConfig()) :?> CloudValue<'S>
            else
                raise <| new InvalidCastException()
            

and [<Sealed; DataContract>]
  private StoreCloudArray<'T>(id:CachedEntityId, elementCount:int, config : LocalStoreCloudValueConfiguration) =
    inherit StoreCloudValue<'T []>(id, elementCount, typeof<'T[]>, config)

    interface CloudArray<'T> with
        member x.Length = x.ElementCount

    interface seq<'T> with
        member x.GetEnumerator() = (x :> CloudValue<'T []>).Value.GetEnumerator()
        member x.GetEnumerator() = ((x :> CloudValue<'T []>).Value :> seq<'T>).GetEnumerator()

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
            let! v = (x :> CloudValue<'T []>).GetValueAsync()
            return v :> seq<'T>
        }

/// CloudValue provider implementation that is based on cloud storage.
and [<Sealed; DataContract>] StoreCloudValueProvider private (config : LocalStoreCloudValueConfiguration) =

    static let isSupportedLevel (level : StorageLevel) =
        level.HasFlag StorageLevel.Disk || 
        level.HasFlag StorageLevel.Memory || 
        level.HasFlag StorageLevel.MemorySerialized

    [<DataMember(Name = "Configuration")>]
    let globalConfig = config.Global

    [<IgnoreDataMember>]
    let mutable config = Some config
    let getConfig() =
        match config with
        | Some cfg -> cfg
        | None -> invalidOp <| sprintf "StoreCloudValue '%s' not installed in the local context." globalConfig.Id

    let tryGetPersistedCloudValueByPath(path : string) = async {
        let config = getConfig()
        let! header = config.Global.MainStore.TryGetHeader(config.Global.Serializer, path)
        match header with
        | None -> return None
        | Some hd -> 
            let ceid = Cached(hd.Level, hd.Hash)
            let cv = StoreCloudValue<_>.CreateReflected(ceid, hd.Count, hd.Type, hd.Type, config)
            return Some cv
    }

    let getPersistedCloudValueByPath(path : string) = async {
        let! result = tryGetPersistedCloudValueByPath path
        return
            match result with
            | Some cv -> cv
            | None -> raise <| new ObjectDisposedException(path, "CloudValue could not be located in store.")
    }

    /// persist value to disk
    let persistValue (level : StorageLevel) (hash : HashResult) (elementCount : int) (value : obj) = async {
        let config = getConfig()
        let header = { Type = getReflectedType value ; Hash = hash ; Level = level ; Count = elementCount }
        match config.LocalStore with
        | Some lc -> ignore <| Async.StartAsTask(lc.Serialize(config.Global.Serializer, header, value, overwrite = false))
        | None -> ()

        do! config.Global.MainStore.Serialize(config.Global.Serializer, header, value, overwrite = false)
    }

    let createCloudValue (level : StorageLevel) (value:'T) = async {
        let config = getConfig()
        let value = box value
        let ceid = CachedEntityId.FromObject(value, level, config.Global.EncapsulationTreshold)
        let elementCount = match value with :? System.Array as a when a.Rank = 1 -> a.Length | _ -> 1

        match ceid with
        | Cached (level, hash) ->
            // persist synchronously to disk if Storage level is disk
            if level.HasFlag StorageLevel.Disk then do! persistValue level hash elementCount value

            // if shadow persisting is supported, write to disk asynchronously
            // TODO: investigate if asynchronous persists should be sent to a queue?
            elif config.Global.ShadowPersistObjects then
                ignore <| Async.StartAsTask(persistValue level hash elementCount value)

            // persist to cache
            if level.HasFlag StorageLevel.MemorySerialized || level.HasFlag StorageLevel.Memory then
                ignore <| config.LocalCache.Add(hash.Id, CachedValue.Create(value, level))

        | _ -> ()

        return StoreCloudValue<_>.CreateReflected(ceid, elementCount, typeof<'T>, getReflectedType value, config) :?> CloudValue<'T>
    }

    /// <summary>
    ///     Initializes a CloudValueProvider instance that is based on a cloud storage service.
    /// </summary>
    /// <param name="storeConfig">Main CloudFileStore configuration used for persisting values.</param>
    /// <param name="cacheFactory">User-supplied InMemory cache factory. Must be serializable.</param>
    /// <param name="localFileStore">Optional local file store factory used for caching persisted values. Defaults to none.</param>
    /// <param name="serializer">Serializer instance used for persisting values. Defaults to FsPickler.</param>
    /// <param name="encapsulationThreshold">Values less than this size will be encapsulated in CloudValue instance. Defaults to 64KiB.</param>
    /// <param name="shadowPersistObjects">Asynchronously persist values to store, even if StorageLevel is declared memory only.</param>
    static member InitCloudValueProvider(mainStore:CloudFileStoreConfiguration, ?cacheFactory : unit -> InMemoryCache, 
                                            ?localFileStore:(unit -> CloudFileStoreConfiguration), ?serializer:ISerializer, 
                                            ?encapsulationThreshold:int64, ?shadowPersistObjects:bool) =

        let id = sprintf "%s:%s/%s" mainStore.FileStore.Name mainStore.FileStore.Id mainStore.DefaultDirectory
        let serializer = match serializer with Some s -> s | None -> new FsPicklerBinaryStoreSerializer() :> ISerializer
        let encapsulationThreshold = defaultArg encapsulationThreshold (64L * 1024L)
        FsPickler.EnsureSerializable ((cacheFactory, localFileStore))
        let config = 
            { 
                Id = id
                MainStore = mainStore
                LocalStore = localFileStore
                Serializer = serializer
                CacheFactory = cacheFactory
                EncapsulationTreshold = encapsulationThreshold
                ShadowPersistObjects = defaultArg shadowPersistObjects true
            }

        let localConfig = StoreCloudValueRegistry.Install config
        new StoreCloudValueProvider(localConfig)

    [<OnDeserialized>]
    member private __.OnDeserialized(_ : StreamingContext) =
        match StoreCloudValueRegistry.TryGetConfiguration(globalConfig.Id) with
        | Some lc -> config <- Some lc
        | None -> ()

    /// Installs InMemory cache for StoreCloudValue instance in the local context
    member __.InstallCacheOnLocalAppDomain() =
        lock globalConfig (fun () ->
            match config with
            | Some _ -> ()
            | None ->
                let localConfig = StoreCloudValueRegistry.Install globalConfig
                config <- Some localConfig)

    /// Uninstalls InMemory cache for StoreCloudValue instance in the local context
    member __.UninstallCacheFromLocalAppDomain () =
        lock globalConfig (fun () ->
            match config with
            | None -> ()
            | Some _ -> 
                ignore <| StoreCloudValueRegistry.Uninstall globalConfig.Id
                config <- None)

    interface ICloudValueProvider with
        member x.Id: string = 
            sprintf "StoreCloudValue provider [%s] at %s." globalConfig.MainStore.FileStore.Id globalConfig.MainStore.DefaultDirectory

        member x.Name: string = "StoreCloudValue"
        member x.DefaultStorageLevel = StorageLevel.MemoryAndDisk
        member x.IsSupportedStorageLevel (level : StorageLevel) = isSupportedLevel level
        member x.GetCloudValueId(value : 'T) = let hash = VagabondRegistry.Instance.Serializer.ComputeHash value in hash.Id
        member x.CreateCloudArrayPartitioned(values : seq<'T>, partitionThreshold : int64, level : StorageLevel) = async {
            let _ = getConfig()
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Not supported storage level '%O'." level
            let createValue (ts:'T[]) = async { let! cv = createCloudValue level ts in return cv :?> CloudArray<'T> }
            return! FsPickler.PartitionSequenceBySize(values, partitionThreshold, createValue, maxConcurrentContinuations = Environment.ProcessorCount)
        }

        member x.CreateCloudValue(payload: 'T, level : StorageLevel): Async<CloudValue<'T>> = async {
            let _ = getConfig()
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Not supported storage level '%O'." level
            return! createCloudValue level payload
        }
        
        member x.Dispose(value: ICloudValue): Async<unit> = async {
            let _ = getConfig()
            return! value.Dispose()
        }
        
        member x.DisposeAllValues(): Async<unit> = async {
            let config = getConfig()
            let! files = config.Global.MainStore.FileStore.EnumerateFiles config.Global.MainStore.DefaultDirectory
            return!
                files
                |> Seq.filter (fun f -> f.EndsWith persistFileSuffix)
                |> Seq.map config.Global.MainStore.FileStore.DeleteFile
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member x.GetAllCloudValues(): Async<ICloudValue []> = async {
            let config = getConfig()
            let! files = config.Global.MainStore.FileStore.EnumerateFiles config.Global.MainStore.DefaultDirectory
            let! cvalues =
                files
                |> Seq.filter (fun f -> f.EndsWith persistFileSuffix)
                |> Seq.map (getPersistedCloudValueByPath >> Async.Catch)
                |> Async.Parallel

            return cvalues |> Array.choose (function Choice1Of2 v -> Some v | _ -> None)
        }

        member x.TryGetCloudValueById(id:string) : Async<ICloudValue option> = async {
            let config = getConfig()
            let hash = HashResult.Parse id
            match config.LocalCache.TryFind hash.Id with
            // Step 1. attempt to resolve CloudValue from local cache state
            | Some (:? CachedValue as cv) when not <| let l = cv.Level in not <| l.HasFlag StorageLevel.Disk ->
                let value = cv.Value
                let count = match value with :? System.Array as a when a.Rank = 1 -> a.Length | _ -> 1
                let reflectedType = getReflectedType value
                let scv = StoreCloudValue<_>.CreateReflected(Cached (cv.Level, hash), count, reflectedType, reflectedType, config)
                return Some scv

            // Step 2. attempt to look up from store
            | _ ->
                let path = config.Global.MainStore.GetPathByHash hash
                try return! tryGetPersistedCloudValueByPath path
                with :? System.IO.FileNotFoundException -> return None
        }