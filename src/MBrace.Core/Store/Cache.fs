namespace MBrace.Store

/// Object caching abstraction
type IObjectCache =

    /// <summary>
    ///     Returns true iff key is contained in cache.
    /// </summary>
    /// <param name="key"></param>
    abstract ContainsKey : key:string -> bool

    /// <summary>
    ///     Adds a key/value pair to cache.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    abstract Add : key:string * value:obj -> bool

    /// <summary>
    ///     Attempt to recover value of given type from cache.
    /// </summary>
    /// <param name="key"></param>
    abstract TryFind : key:string -> obj option