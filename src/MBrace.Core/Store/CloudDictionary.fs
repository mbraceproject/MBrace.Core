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
    abstract ContainsKeyAsync : key:string -> Async<bool>

    /// <summary>
    ///     Try adding a new key-value pair to dictionary.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract TryAddAsync : key:string * value:'T -> Async<bool>

    /// <summary>
    ///     Add a new key-value pair to dictionary, overwriting if one already exists.
    /// </summary>
    /// <param name="key">Key for entry.</param>
    /// <param name="value">Value for entry.</param>
    abstract AddAsync : key:string * value:'T -> Async<unit>

    /// <summary>
    ///     Performs an atomic transaction on value of given key.
    /// </summary>
    /// <param name="key">Key to be transacted.</param>
    /// <param name="transacter">Transaction function.</param>
    /// <param name="maxRetries">Maximum number of retries. Defaults to infinite.</param>
    abstract TransactAsync : key:string * transacter:('T option -> 'R * 'T) * ?maxRetries:int -> Async<'R>

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    ///     Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    abstract RemoveAsync : key:string -> Async<bool>

    /// <summary>
    ///     Attempt reading a value of provided key from dictionary.
    /// </summary>
    /// <param name="key">Key to be read.</param>
    abstract TryFindAsync : key:string -> Async<'T option>

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

open System.ComponentModel
open System.Collections.Generic
open System.Runtime.CompilerServices

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
        return! Cloud.OfAsync <| provider.CreateDictionary<'T> dictionaryId
    }

    /// <summary>
    ///     Attempt to recover an already existing CloudDictionary of provided Id and type.
    /// </summary>
    /// <param name="dictionaryId">CloudDictionary unique identifier.</param>
    static member GetById<'T>(dictionaryId : string) = local {
        let! provider = Cloud.GetResource<ICloudDictionaryProvider>()
        return! Cloud.OfAsync <| provider.GetDictionaryById<'T> dictionaryId
    }


[<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
type CloudDictionaryExtensions =

    /// <summary>
    ///     Checks if supplied key is contained in dictionary.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    [<Extension>]
    static member ContainsKey (this : CloudDictionary<'T>, key : string) = local {
        return! Cloud.OfAsync <| this.ContainsKeyAsync key
    }

    /// <summary>
    ///     Try adding a new key/value pair to dictionary. Returns true if successful.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    [<Extension>]
    static member TryAdd (this : CloudDictionary<'T>, key : string, value : 'T) = local {
        return! Cloud.OfAsync <| this.TryAddAsync(key, value)
    }

    /// <summary>
    ///     Force add a new key/value pair to dictionary.
    /// </summary>
    /// <param name="key">Key to be added.</param>
    /// <param name="value">Value to be added.</param>
    [<Extension>]
    static member Add (this : CloudDictionary<'T>, key : string, value : 'T) : CloudLocal<unit>  = local {
        return! Cloud.OfAsync <| this.AddAsync(key, value)
    }

    /// <summary>
    ///     Atomically adds or updates a key/value entry.
    ///     Returns the updated value.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Updater function.</param>
    [<Extension>]
    static member AddOrUpdate (this : CloudDictionary<'T>, key : string, updater : 'T option -> 'T) : CloudLocal<'T> = local {
        let transacter (curr : 'T option) = let t = updater curr in t, t
        return! Cloud.OfAsync <| this.TransactAsync(key, transacter)
    }

    /// <summary>
    ///     Atomically updates a key/value entry.
    ///     Returns the updated value.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    /// <param name="updater">Entry updater function.</param>
    [<Extension>]
    static member Update (this : CloudDictionary<'T>, key : string, updater : 'T -> 'T) = local {
        let transacter (curr : 'T option) =
            match curr with
            | None -> invalidOp <| sprintf "No value of key '%s' was found in dictionary." key
            | Some t -> let t' = updater t in t', t'

        return! Cloud.OfAsync <| this.TransactAsync(key, transacter)
    }

    /// <summary>
    ///     Try reading value for entry of supplied key.
    /// </summary>
    /// <param name="key">Key to entry.</param>
    [<Extension>]
    static member TryFind (this : CloudDictionary<'T>, key : string) : CloudLocal<'T option> = local {
        return! Cloud.OfAsync <| this.TryFindAsync key
    }

    /// <summary>
    ///     Removes entry of supplied key from dictionary.
    /// </summary>
    /// <param name="key">Key to be removed.</param>
    /// <param name="dictionary">Dictionary to be updated.</param>
    [<Extension>]
    static member Remove (this : CloudDictionary<'T>, key : string) : CloudLocal<bool> = local {
        return! Cloud.OfAsync <| this.RemoveAsync(key)
    }

    /// <summary>
    ///     Performs a transaction on value contained in provided key
    /// </summary>
    /// <param name="key">Key to perform transaction on.</param>
    /// <param name="transacter">Transaction funtion.</param>
    [<Extension>]
    static member Transact (this : CloudDictionary<'T>, key : string, transacter : 'T -> 'R * 'T) : CloudLocal<'R> = local {
        let transacter (curr : 'T option) =
            match curr with
            | None -> invalidOp <| sprintf "No value of key '%s' was found in dictionary." key
            | Some t -> let r,t' = transacter t in r, t'

        return! Cloud.OfAsync <| this.TransactAsync(key, transacter)
    }