namespace MBrace

open System.Runtime.Serialization

open MBrace.Continuation
open MBrace.Store

#nowarn "444"

/// Defines a defered computation whose result can be
/// cached in-memory and on-demand to worker machines.
[<Sealed; DataContract>]
type CloudCacheable<'T> internal (evaluator : Local<'T>, ?cacheByDefault : bool) =

    [<DataMember(Name = "CacheId")>]
    let id = System.Guid.NewGuid().ToString()
    [<DataMember(Name = "Evaluator")>]
    let evaluator = evaluator
    [<DataMember(Name = "CacheByDefault")>]
    let mutable cacheByDefault = defaultArg cacheByDefault false

    /// Gets or sets the default caching behaviour.
    member __.CacheByDefault
        with get () = cacheByDefault
        and set c = cacheByDefault <- c

    /// <summary>
    ///     Attempt to cache computation to local execution context.
    ///     Returns true if succesful or already cached.
    /// </summary>
    member __.PopulateCache() : Local<bool> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        match config.Cache with
        | None -> return false
        | Some c ->
            if c.ContainsKey id then return true
            else
                let! value = evaluator
                return c.Add(id, value)
    }

    /// <summary>
    ///     Returns true if value is cached in the local execution context.
    /// </summary>
    member __.IsCachedLocally : Local<bool> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        return config.Cache |> Option.exists (fun c -> c.ContainsKey id)
    }

    /// <summary>
    ///     Evaluates the entity, returning the locally cached value if it already exists.
    /// </summary>
    member __.Value : Local<'T> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        match config.Cache with
        | None -> return! evaluator
        | Some c ->
            match c.TryFind id with
            | Some (:? 'T as t) -> return t
            | _ -> 
                let! t = evaluator
                if cacheByDefault then ignore(c.Add(id, t))
                return t
    }


type CloudCacheable =

    /// <summary>
    ///     Creates a computation that can be cached on demand to worker instances.
    /// </summary>
    /// <param name="evaluator">Evaluator workflow.</param>
    /// <param name="cacheByDefault">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member Create(evaluator : Local<'T>, ?cacheByDefault : bool) = new CloudCacheable<'T>(evaluator, ?cacheByDefault = cacheByDefault)

    /// <summary>
    ///     Creates a computation that can be cached on demand to worker instances.
    /// </summary>
    /// <param name="evaluator">Evaluator workflow.</param>
    /// <param name="cacheByDefault">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member Create(evaluator : unit -> Local<'T>, ?cacheByDefault : bool) = new CloudCacheable<'T>(local.Delay evaluator, ?cacheByDefault = cacheByDefault)