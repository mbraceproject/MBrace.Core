namespace Nessos.MBrace.Store

/// In-memory caching abstraction
type IInMemoryCache =

    abstract ContainsKey : key:string -> bool

    /// <summary>
    ///     Attempt to add key/value pair to cache.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    abstract TryAdd<'T> : key:string * value:'T -> bool

    /// <summary>
    ///     Attempt to recover value of given type from cache.
    /// </summary>
    /// <param name="key"></param>
    abstract TryFind<'T> : key:string -> 'T option

/// In-Memory cache registration point
type InMemoryCacheRegistry private () =
    static let mutable cache : IInMemoryCache option = None

    /// Gets the global In-Memory cache.
    static member InstalledCache = cache
    /// Sets the global In-Memory cache.
    static member SetCache c = cache <- Some c