namespace MBrace.Store

open System.Collections.Generic

open MBrace.Core
open MBrace.Core.Internals

/// Distributed key-value collection.
type ICloudDictionary<'T> =
    inherit ICloudDisposable
    inherit ICloudCollection<KeyValuePair<string,'T>>

    /// Dictionary identifier
    abstract Id : string

    /// <summary>
    ///     Checks if entry of supplied key exists in dictionary.
    /// </summary>
    /// <param name="key">Input key.</param>
    abstract ContainsKey : key:string -> Local<bool>

    /// <summary>
    ///     Try adding a new key-value pair to dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract TryAdd : key:string * value:'T -> Local<bool>

    /// <summary>
    ///     Add a new key-value pair to dictionary, overwriting if one already exists.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract Add : key:string * value:'T -> Local<unit>

    /// <summary>
    ///     Atomically adds or updates a value in dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="updater">Entry updater function.</param>
    abstract AddOrUpdate : key:string * updater:('T option -> 'T) -> Local<'T>

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    ///     Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    abstract Remove : key:string -> Local<bool>

    /// <summary>
    ///     Attempt reading a value of provided key from dictionary.
    /// </summary>
    /// <param name="key">Key to be read.</param>
    abstract TryFind : key:string -> Local<'T option>

namespace MBrace.Store.Internals

open MBrace.Core
open MBrace.Store

/// Abstract factory for ICloudDictionary.
type ICloudDictionaryProvider =
    /// <summary>
    ///     Checks if supplied value is supported by Dictionary implementation.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSupportedValue : value:'T -> bool
    /// Create a new ICloudDictionary instance.
    abstract Create<'T> : unit -> Async<ICloudDictionary<'T>>

namespace MBrace.Store

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store.Internals

#nowarn "444"

/// CloudDictionary static methods
type CloudDictionary =

    /// <summary>
    ///     Checks if value is supported for dictionary implementation
    ///     in current execution context.
    /// </summary>
    /// <param name="value">Value to be verified.</param>
    static member IsSupportedValue(value : 'T) = local {
        let! config = Cloud.GetResource<ICloudDictionaryProvider>()
        return config.IsSupportedValue value
    }

    /// Creates a new CloudDictionary instance.
    static member New<'T>() = local {
        let! provider = Cloud.GetResource<ICloudDictionaryProvider>()
        return! ofAsync <| provider.Create<'T> ()
    }

    /// <summary>
    ///     Checks if supplied key is contained in dictionary.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    /// <param name="dictionary">Input dictionary.</param>
    static member ContainsKey (key : string) (dictionary : ICloudDictionary<'T>) = local {
        return! dictionary.ContainsKey key
    }

    /// <summary>
    ///     Try adding a new key/value pair to dictionary. Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member TryAdd (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) = local {
        return! dictionary.TryAdd(key, value)
    }

    /// <summary>
    ///     Adds a new key/value pair to dictionary. Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member Add (key : string) (value : 'T) (dictionary : ICloudDictionary<'T>) = local {
        return! dictionary.Add(key, value)
    }

    /// <summary>
    ///     Atomically adds or updates a key/value entry.
    ///     Returns the updated value.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member AddOrUpdate (key : string) (updater : 'T option -> 'T) (dictionary : ICloudDictionary<'T>) = local {
        return! dictionary.AddOrUpdate(key, updater)
    }

    /// <summary>
    ///     Try reading value for entry of supplied key.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    static member TryFind (key : string) (dictionary : ICloudDictionary<'T>) : Local<'T option> = local {
        return! dictionary.TryFind key
    }

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member Remove (key : string) (dictionary : ICloudDictionary<'T>) = local {
        return! dictionary.Remove(key)
    }

    /// Disposes CloudDictionary instance.
    static member Dispose(dictionary : ICloudDictionary<'T>) = 
        local { return! dictionary.Dispose() }