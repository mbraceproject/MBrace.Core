namespace MBrace.Store.Internals

open System
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

#nowarn "444"

/// Object caching abstraction
type IObjectCache =

    /// <summary>
    ///     Returns true iff key is contained in cache.
    /// </summary>
    /// <param name="key"></param>
    abstract ContainsKey : key:string -> Async<bool>

    /// <summary>
    ///     Adds a key/value pair to cache.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    abstract Add : key:string * value:obj -> Async<bool>

    /// <summary>
    ///     Attempt to recover value of given type from cache.
    /// </summary>
    /// <param name="key"></param>
    abstract TryFind : key:string -> Async<obj option>

/// Represents an entity that can be cached across worker instances.
type ICloudCacheable<'T> =
    /// Universal unique identifier for cached entity.
    /// Used to dereference values from local caches.
    abstract UUID : string
    /// Fetches/computes the cacheable value from its source.
    /// This will NOT return return a cached value even if it exists in context.
    abstract GetSourceValue : unit -> Local<'T>

/// CloudCache static methods.
type CloudCache =

    /// <summary>
    ///     Populates cache in the current execution context with value from provided entity. 
    ///     Returns a boolean indicating success of the operation.
    /// </summary>
    /// <param name="entity">Entity to be cached.</param>
    static member PopulateCache(entity : ICloudCacheable<'T>) : Local<bool> = local {
        let! cache = Cloud.TryGetResource<IObjectCache> ()
        match cache with
        | None -> return false
        | Some c ->
            let! containsKey = c.ContainsKey entity.UUID
            if containsKey then return true
            else
                let! value = entity.GetSourceValue()
                return! c.Add (entity.UUID, value)
    }

    /// <summary>
    ///     Checks if entity is cached in the local execution context.
    /// </summary>
    /// <param name="entity">Cacheable entity.</param>
    static member IsCachedEntity(entity : ICloudCacheable<'T>) : Local<bool> = local {
        let! cache = Cloud.TryGetResource<IObjectCache> ()
        match cache with
        | None -> return false
        | Some c -> return! c.ContainsKey entity.UUID
    }

    /// <summary>
    ///     Attempts to get value from local context cache only.
    ///     Returns 'Some' if cached and 'None' if not cached.
    /// </summary>
    /// <param name="entity">Cacheable entity.</param>
    static member TryGetCachedValue(entity : ICloudCacheable<'T>) : Local<'T option> = local {
        let! cache = Cloud.TryGetResource<IObjectCache> ()
        match cache with
        | None -> return None
        | Some c ->
            let! cacheResult = c.TryFind entity.UUID
            match cacheResult with
            | None -> return None
            | Some(:? 'T as t) -> return Some t
            | Some null -> return raise <| new NullReferenceException("CloudCache entity.")
            | Some o -> 
                let msg = sprintf "CloudCache: Expected cached type '%O' but was '%O'." typeof<'T> (o.GetType())
                return raise <| new InvalidCastException(msg)
    }

    /// <summary>
    ///     Gets cached value from local execution context.
    ///     If not found in cache, will fetch from source and cache now.
    /// </summary>
    /// <param name="entity">Cacheable entity.</param>
    /// <param name="cacheIfNotExists">Populate cache now if not found. Defaults to true.</param>
    static member GetCachedValue(entity : ICloudCacheable<'T>, ?cacheIfNotExists : bool) : Local<'T> = local {
        let cacheIfNotExists = defaultArg cacheIfNotExists true
        let! cache = Cloud.TryGetResource<IObjectCache> ()
        match cache with
        | None -> return! entity.GetSourceValue()
        | Some c ->
            let! cacheResult = c.TryFind entity.UUID
            match cacheResult with
            | Some(:? 'T as t) -> return t
            | Some null -> return raise <| new NullReferenceException("CloudCache entity.")
            | Some o -> 
                let msg = sprintf "CloudCache: Expected cached type '%O' but was '%O'." typeof<'T> (o.GetType())
                return raise <| new InvalidCastException(msg)
            | None ->
                let! t = entity.GetSourceValue()
                if cacheIfNotExists then
                    let! _ = c.Add(entity.UUID, t) in ()

                return t
    }