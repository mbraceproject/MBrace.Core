namespace MBrace.Runtime.Utils

open System
open System.Collections.Generic
open System.Collections.Specialized
open System.Runtime.Caching

open MBrace.Core
open MBrace.Core.Internals

/// In-Memory caching mechanism using System.Runtime.Caching.MemoryCache
type InMemoryCache private (name : string, config : NameValueCollection) =
    let cache = new MemoryCache(name, config)
    let policy = new CacheItemPolicy()

    /// Cache instance identifier
    member __.Name = name

    /// <summary>
    ///     Creates a new in-memory cache instance.
    /// </summary>
    /// <param name="name">Cache name. Defaults to self-assigned.</param>
    /// <param name="physicalMemoryLimitPercentage">Physical memory percentage threshold. Defaults to 60.</param>
    static member Create(?name, ?physicalMemoryLimitPercentage : int) =
        let name =
            match name with
            | None -> mkUUID()
            | Some n -> n

        let percentage = 
            match defaultArg physicalMemoryLimitPercentage 60 with
            | n when n > 0 && n <= 100 -> n
            | _ -> invalidArg "physicalMemoryLimitPercentage" "must be between 1 and 100."

        let config = new NameValueCollection()
        do config.Add("PhysicalMemoryLimitPercentage", percentage.ToString())
        new InMemoryCache(name, config)

    /// <summary>
    ///     Look up cached entry by key.
    /// </summary>
    /// <param name="key">Key</param>
    member self.TryFind (key : string) : obj option =
        if cache.Contains key then Some cache.[key]
        else None

    /// <summary>
    ///     Look up cached entry by key.
    /// </summary>
    /// <param name="key">Key</param>
    member self.Get (key : string) : obj = cache.Get key

    /// <summary>
    ///     Checks if cache contains provided key.
    /// </summary>
    /// <param name="key"></param>
    member self.ContainsKey (key : string) : bool =
        cache.Contains key

    /// <summary>
    ///     Try adding a new key to cache.
    /// </summary>
    /// <param name="key">Key.</param>
    /// <param name="value">Value.</param>
    member self.Add(key : string, value : obj) : bool =
        if obj.ReferenceEquals(value, null) then false
        else
            try cache.Add(key, value, policy)
            with :? OutOfMemoryException -> 
                cache.Trim(20) |> ignore
                self.Add(key, value)
    
    /// <summary>
    ///     Delete existing key from cache.
    /// </summary>
    /// <param name="key">Key.</param>
    member self.Delete(key : string) = 
        if cache.Contains key then
            let _ = cache.Remove(key) in true
        else
            false