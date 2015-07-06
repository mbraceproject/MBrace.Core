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
open MBrace.Runtime.Vagabond

// StoreCloudValue instances have three possible representations
//  * Encapsulated: values small enough to be encapsulated in the CloudValue instance
//  * Cached: values that are persisted in store and cached in-memory
//  * VagabondValue: large values that are already managed by Vagabond and are therefore available in the local context
[<AutoSerializable(true); NoEquality; NoComparison>]
type private CachedEntityId =
    | Encapsulated of value:obj * hash:HashResult
    | Persisted of id:string * hash:HashResult
    | VagabondValue of hash:HashResult
with
    /// Gets the FsPickler hashcode of the cached value
    member c.Hash =
        match c with
        | Encapsulated (_,hash) -> hash
        | Persisted (_,hash) -> hash
        | VagabondValue hash -> hash

    /// <summary>
    ///     Creates a cache entity identifier for provided object graph.
    /// </summary>
    /// <param name="obj">Input serializable object graph.</param>
    /// <param name="sizeThreshold">Size threshold in bytes. Objects less than the treshold will be encapsulated in CloudValue instance.</param>
    static member FromObject(obj:obj, sizeThreshold:int64) : CachedEntityId =
        let vgb = VagabondRegistry.Instance
        let hash = vgb.Serializer.ComputeHash obj
        if hash.Length <= sizeThreshold then Encapsulated(obj, hash)
        else
            match vgb.TryGetBindingByHash hash with
            | Some _ -> VagabondValue hash
            | None ->
                let truncate (n : int) (txt : string) =
                    if n <= txt.Length then txt
                    else txt.Substring(0, n)

                let base32Enc = Convert.BytesToBase32 (Array.append (BitConverter.GetBytes hash.Length) hash.Hash)
                let id = sprintf "%s-%s" (truncate 7 hash.Type) base32Enc
                Persisted (id, hash)

/// Header value serialized at the beginning of the persisted file
[<NoEquality; NoComparison>]
type private CloudValueHeader =
    {
        Id : string
        Type : Type
        Hash : HashResult
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
    ///     Gets store path for given cached entity id.
    /// </summary>
    /// <param name="id">Cache identifier.</param>
    member c.GetPath(id : string) =
        c.FileStore.Combine(c.StoreContainer, id)

/// Registry for StoreCloudValue configurations; used for StoreCloudValue deserialization
type private StoreCloudValueRegistry private () =
    static let container = new ConcurrentDictionary<string, StoreCloudValueConfiguration> ()
    static member Register(config : StoreCloudValueConfiguration) = container.TryAdd(config.Id, config)
    static member TryGetById(id : string) = 
        let ok,value = container.TryGetValue id
        if ok then Some value
        else None

    static member RemoveById(id : string) = container.TryRemove id

/// Serializable, store-based CloudValue implementation
[<Sealed; DataContract>]
type private StoreCloudValue<'T>(id:CachedEntityId, config : StoreCloudValueConfiguration) =

    [<DataMember(Name = "Id")>]
    let id = id

    [<DataMember(Name = "InMemory")>]
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
        | Persisted (id,_) ->
            let config = getConfig()
            match config.LocalCache.TryFind id with
            | Some o -> return o :?> 'T
            | None ->
                let filePath = config.GetPath id
                use! fs = config.FileStore.BeginRead filePath
                let _ = config.Serializer.Deserialize<CloudValueHeader>(fs, leaveOpen = true)
                let value = config.Serializer.Deserialize<'T>(fs, leaveOpen = true)
                return
                    if config.LocalCache.Add(id, value) then value
                    else config.LocalCache.Get id :?> 'T
    }

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        config <- StoreCloudValueRegistry.TryGetById configId

    member internal __.Dispose() = async {
        match id with
        | Encapsulated _
        | VagabondValue _ -> return ()
        | Persisted (id,_) -> 
            let config = getConfig()
            do! config.FileStore.DeleteFile (config.GetPath id)
    }

    interface ICloudValue<'T> with
        member x.Dispose(): Local<unit> = local {
            return! x.Dispose()
        }
        
        member x.GetBoxedValueAsync(): Async<obj> = async {
            let! t = getValue ()
            return box t
        }
        
        member x.GetValueAsync(): Async<'T> = getValue()
        
        member x.Id: string =
            match id with
            | Persisted (id,_) -> id
            | Encapsulated _ -> "Small object encapsulated in CloudValue instance."
            | VagabondValue _ -> "Value cached by Vagabond."
        
        member x.IsCachedLocally: bool = 
            match id with
            | Encapsulated _ -> true
            | VagabondValue _ -> true
            | Persisted (id,_) -> getConfig().LocalCache.ContainsKey id
        
        member x.Size: int64 = id.Hash.Length
        
        member x.StorageLevel: StorageLevel = StorageLevel.MemoryAndDisk
        
        member x.Type: Type = typeof<'T>
        
        member x.Value: 'T =
            getValue() |> Async.RunSync
        
        member x.ValueBoxed: obj = 
            getValue() |> Async.RunSync |> box

/// CloudValue provider implementation that is based on cloud storage
[<AutoSerializable(false)>]
type StoreCloudValueProvider private (config : StoreCloudValueConfiguration) =
    let mutable isDisposed = false
    let ensureActive() =
        if isDisposed then
            raise <| new ObjectDisposedException("StoreCloudValueProvider has been disposed.")

    let getValueByPath(path : string) = async {
        use! stream = config.FileStore.BeginRead path
        try
            let header = config.Serializer.Deserialize<CloudValueHeader>(stream, leaveOpen = true)
            let e = Existential.FromType header.Type
            let ceid = Persisted(header.Id, header.Hash)
            return e.Apply { new IFunc<ICloudValue> with member __.Invoke<'T>() = new StoreCloudValue<'T>(ceid, config) :> ICloudValue }
        with e ->
            return raise <| new FormatException(sprintf "Could not read CloudValue from '%s'." path, e)
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

        member x.CreateCloudValue(payload: 'T): Async<ICloudValue<'T>> = async {
            ensureActive()
            let ceid = CachedEntityId.FromObject(payload, config.EncapsulatationTreshold)
            match ceid with
            | Encapsulated _ -> return ()
            | VagabondValue _ -> return ()
            | Persisted (id,hash) ->
                let path = config.GetPath id
                let! exists = config.FileStore.FileExists path
                if not exists then
                    use! stream = config.FileStore.BeginWrite path
                    let header = { Type = typeof<'T> ; Id = id ; Hash = hash }
                    config.Serializer.Serialize<CloudValueHeader>(stream, header, leaveOpen = true)
                    config.Serializer.Serialize<'T>(stream, payload, leaveOpen = true)

            let cv = new StoreCloudValue<'T>(ceid, config)
            return cv :> ICloudValue<'T>
        }
        
        member x.Dispose(value: ICloudValue): Async<unit> = async {
            ensureActive()
            let e = Existential.FromType value.Type
            return! e.Apply { new IAsyncFunc<unit> with member __.Invoke<'T>() = (value :?> StoreCloudValue<'T>).Dispose() }
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
                |> Seq.map (getValueByPath >> Async.Catch)
                |> Async.Parallel

            return cvalues |> Array.choose (function Choice1Of2 v -> Some v | _ -> None)
        }

        member x.GetById(id:string) : Async<ICloudValue> = async {
            ensureActive()
            let path = config.FileStore.Combine(config.StoreContainer, id)
            return! getValueByPath path
        }