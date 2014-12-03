namespace Nessos.MBrace

/// Represent a distributed atomically updatable value reference
type ICloudAtom<'T> =
    inherit ICloudDisposable
    /// Returns the current value of atom.
    abstract GetValue : unit -> Async<'T>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="updater">Value updating function</param>
    /// <param name="maxRetries">Maximum retries under optimistic semantics. Defaults to infinite.</param>
    abstract Update : updater:('T -> 'T) * ?maxRetries:int -> Async<unit>

    /// <summary>
    ///      Forces a value on atom.
    /// </summary>
    /// <param name="value">value to be set.</param>
    abstract Force : value:'T -> Async<unit>

namespace Nessos.MBrace.Store
 
open Nessos.MBrace

/// Defines a factory for distributed atoms
type ICloudAtomProvider =

    /// unique cloud file store identifier
    abstract Id : string

    /// Creates a new atom instance for given type
    abstract CreateAtom<'T> : unit -> Async<ICloudAtom<'T>>

///// Defines a cloud table storage abstraction
//type ICloudTableStore =
//
//    /// Unique table store identifier
//    abstract UUID : string
//
//    /// Returns a serializable table store factory for the current instance.
//    abstract GetFactory : unit -> ICloudTableStoreFactory
//
//    /// <summary>
//    ///     Checks if provided value is suitable for table storage
//    /// </summary>
//    /// <param name="value">Value to be checked.</param>
//    abstract IsSupportedValue : value:'T -> bool
//
//    abstract GetPartitionId : id:string -> string
//
//    /// <summary>
//    ///     Checks if entry with provided key exists.
//    /// </summary>
//    /// <param name="id">Entry id.</param>
//    abstract Exists : id:string -> Async<bool>
//
//    /// <summary>
//    ///     Deletes entry with provided key.
//    /// </summary>
//    /// <param name="id"></param>
//    abstract Delete : id:string -> Async<unit>
//
//    /// <summary>
//    ///     Creates a new entry with provided initial value.
//    ///     Returns the key identifier for new entry.
//    /// </summary>
//    /// <param name="initial">Initial value.</param>
//    abstract Create<'T> : container:string * initial:'T -> Async<string>
//
//    /// <summary>
//    ///     Returns the current value of provided atom.
//    /// </summary>
//    /// <param name="id">Entry identifier.</param>
//    abstract GetValue<'T> : id:string -> Async<'T>
//
//    /// <summary>
//    ///     Atomically updates table entry of given id using updating function.
//    /// </summary>
//    /// <param name="id">Entry identifier.</param>
//    /// <param name="updater">Updating function.</param>
//    abstract Update : id:string * updater:('T -> 'T) -> Async<unit>
//
//    /// <summary>
//    ///     Force update of existing table entry with given value.
//    /// </summary>
//    /// <param name="id">Entry identifier.</param>
//    /// <param name="value">Value to be set.</param>
//    abstract Force : id:string * value:'T -> Async<unit>
//
//    abstract EnumeratePartitions : unit -> Async<string []>
//
//    /// <summary>
//    ///     Enumerates all keys.
//    /// </summary>
//    abstract EnumerateKeys : partitionId:string -> Async<string []>
//
///// Defines a serializable abstract factory for a table store instance.
///// Used for pushing filestore definitions across machines
//and ICloudTableStoreFactory =
//    abstract Create : unit -> ICloudTableStore