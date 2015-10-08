namespace MBrace.Core

open System.Collections.Generic

open MBrace.Core
open MBrace.Core.Internals

/// Distributed key-value collection.
type CloudDictionary<'T> =
    inherit ICloudDisposable
    inherit ICloudCollection<KeyValuePair<string,'T>>

    /// Dictionary identifier
    abstract Id : string

    /// <summary>
    ///     Checks if entry of supplied key exists in dictionary.
    /// </summary>
    /// <param name="key">Input key.</param>
    abstract ContainsKey : key:string -> Async<bool>

    /// <summary>
    ///     Try adding a new key-value pair to dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract TryAdd : key:string * value:'T -> Async<bool>

    /// <summary>
    ///     Add a new key-value pair to dictionary, overwriting if one already exists.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract Add : key:string * value:'T -> Async<unit>

    /// <summary>
    ///     Performs an atomic transaction on value of given key.
    /// </summary>
    /// <param name="key">Key to be transacted.</param>
    /// <param name="transacter">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries. Defaults to infinite.</param>
    abstract Transact : key:string * transacter:('T option -> 'R * 'T) * ?maxRetries:int -> Async<'R>

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    ///     Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    abstract Remove : key:string -> Async<bool>

    /// <summary>
    ///     Attempt reading a value of provided key from dictionary.
    /// </summary>
    /// <param name="key">Key to be read.</param>
    abstract TryFind : key:string -> Async<'T option>

namespace MBrace.Core.Internals

open MBrace.Core
open MBrace.Core

/// Abstract factory for ICloudDictionary.
type ICloudDictionaryProvider =

    /// CloudDictionary implementation name
    abstract Name : string

    /// CloudDictionary provider instance id
    abstract Id : string

    /// Creates a unique CloudDictionary identifier
    abstract GetRandomDictionaryId : unit -> string

    /// <summary>
    ///     Checks if supplied value is supported by Dictionary implementation.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSupportedValue : value:'T -> bool

    /// <summary>
    ///   Create a new CloudDictionary instance.  
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    abstract CreateDictionary<'T> : dictionaryId:string -> Async<CloudDictionary<'T>>

    /// <summary>
    ///     Attempt to recover an already existing CloudDictionary of provided Id and type.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    abstract GetDictionaryById<'T> : dictionaryId:string -> Async<CloudDictionary<'T>>

namespace MBrace.Core

open System.Collections.Generic

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals

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

    /// <summary>
    ///     Creates a new CloudDictionary instance.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier. Defaults to randomly generated name.</param>
    static member New<'T>(?dictionaryId : string) = local {
        let! provider = Cloud.GetResource<ICloudDictionaryProvider>()
        let dictionaryId = match dictionaryId with None -> provider.GetRandomDictionaryId() | Some did -> did
        return! provider.CreateDictionary<'T> dictionaryId
    }

    /// <summary>
    ///     Attempt to recover an already existing CloudDictionary of provided Id and type.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    static member GetById<'T>(dictionaryId : string) = local {
        let! provider = Cloud.GetResource<ICloudDictionaryProvider>()
        return! provider.CreateDictionary<'T> dictionaryId
    }

    /// <summary>
    ///     Checks if supplied key is contained in dictionary.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    /// <param name="dictionary">Input dictionary.</param>
    static member ContainsKey (key : string) (dictionary : CloudDictionary<'T>) = local {
        return! dictionary.ContainsKey key
    }

    /// <summary>
    ///     Try adding a new key/value pair to dictionary. Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member TryAdd (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) = local {
        return! dictionary.TryAdd(key, value)
    }

    /// <summary>
    ///     Adds a new key/value pair to dictionary. Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member Add (key : string) (value : 'T) (dictionary : CloudDictionary<'T>) = local {
        return! dictionary.Add(key, value)
    }

    /// <summary>
    ///     Atomically adds or updates a key/value entry.
    ///     Returns the updated value.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Updater function.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member AddOrUpdate (key : string) (updater : 'T option -> 'T) (dictionary : CloudDictionary<'T>) = local {
        let transacter (curr : 'T option) = let t = updater curr in t, t
        return! dictionary.Transact(key, transacter)
    }

    /// <summary>
    ///     Atomically updates a key/value entry.
    ///     Returns the updated value.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="newValue">Value to be inserted in case of missing entry.</param>
    /// <param name="updater">Entry updater function.</param>
    static member Update (key : string) (updater : 'T -> 'T) (dictionary : CloudDictionary<'T>) = local {
        let transacter (curr : 'T option) =
            match curr with
            | None -> invalidOp <| sprintf "No value of key '%s' was found in dictionary." key
            | Some t -> let t' = updater t in t', t'

        return! dictionary.Transact(key, transacter)
    }

    /// <summary>
    ///     Try reading value for entry of supplied key.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="dictionary">Dictionary to be accessed.</param>
    static member TryFind (key : string) (dictionary : CloudDictionary<'T>) : Local<'T option> = local {
        return! dictionary.TryFind key
    }

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    static member Remove (key : string) (dictionary : CloudDictionary<'T>) = local {
        return! dictionary.Remove(key)
    }

    /// <summary>
    ///     Performs a transaction on value contained in provided key
    /// </summary>
    /// <param name="transacter">Transaction funtion.</param>
    /// <param name="key">Key to perform transaction on.</param>
    /// <param name="dictionary">Input dictionary.</param>
    static member Transact (transacter : 'T -> 'R * 'T) (key : string) (dictionary : CloudDictionary<'T>) = local {
        let transacter (curr : 'T option) =
            match curr with
            | None -> invalidOp <| sprintf "No value of key '%s' was found in dictionary." key
            | Some t -> let r,t' = transacter t in r, t'

        return! dictionary.Transact(key, transacter)
    }