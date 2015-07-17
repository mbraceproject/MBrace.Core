namespace MBrace.Core

#nowarn "444"

open System

open MBrace.Core.Internals

/// Storage levels used for caching
type StorageLevel =
    | Memory                    = 1
    | Disk                      = 2
    | MemorySerialized          = 4
    | MemoryAndDisk             = 3
    | MemoryAndDiskSerialized   = 6

/// Serializable entity that represents an immutable 
/// .NET object that has been cached by the MBrace runtime.
type ICloudValue =
    inherit ICloudDisposable
    /// CloudValue identifier.
    abstract Id : string
    /// Gets size of the cached object in bytes.
    abstract Size : int64
    /// Storage level used for value.
    abstract StorageLevel : StorageLevel
    /// Type of CloudValue.
    abstract Type : Type
    /// Determines if cached value already exists
    /// in the local execution context.
    abstract IsCachedLocally : bool
    /// Gets the boxed payload of the CloudValue.
    abstract GetBoxedValue : unit -> obj
    /// Asynchronously gets the boxed payload of the CloudValue.
    abstract GetBoxedValueAsync : unit -> Async<obj>
    /// Casts CloudValue to specified type, if applicable.
    abstract Cast<'S> : unit -> ICloudValue<'S>

/// Serializable entity that represents an immutable 
/// .NET object that has been cached by the MBrace runtime.
and ICloudValue<'T> =
    inherit ICloudValue
    /// Gets the payload of the CloudValue.
    abstract Value : 'T
    /// Asynchronously gets the boxed payload of the CloudValue.
    abstract GetValueAsync : unit -> Async<'T>

/// Serializable entity that represents an immutable 
/// array that has been cached by the MBrace runtime.
type ICloudArray<'T> =
    inherit ICloudValue<'T []>
    inherit ICloudCollection<'T>
    /// Array element count
    abstract Length : int

namespace MBrace.Core.Internals

open MBrace.Core

/// Cloud Value provider implementation.
type ICloudValueProvider =

    /// Implementation name.
    abstract Name : string

    /// CloudValue implementation instance identifier.
    abstract Id : string

    /// Default Storage level used by Cloud Value implementation.
    abstract DefaultStorageLevel : StorageLevel

    /// Checks if provided storage level is supported by implementation.
    abstract IsSupportedStorageLevel : level:StorageLevel -> bool

    /// <summary>
    ///     Initializes a CloudValue with supplied payload.
    /// </summary>
    /// <param name="payload">Payload to be cached.</param>
    /// <param name="storageLevel">Storage level for cloud value.</param>
    abstract CreateCloudValue : payload:'T * storageLevel:StorageLevel -> Async<ICloudValue<'T>>

    /// <summary>
    ///     Initializes a collection of CloudArrays partitioned by size.
    /// </summary>
    /// <param name="payload">Input sequence to be persisted.</param>
    /// <param name="storageLevel">Storage level for cloud arrays.</param>
    /// <param name="partitionThreshold">Partition threshold in bytes. Defaults to infinite threshold.</param>
    abstract CreatePartitionedArray : payload:seq<'T> * storageLevel:StorageLevel * ?partitionThreshold:int64 -> Async<ICloudArray<'T> []>

    /// <summary>
    ///     Gets CloudValue by cache id
    /// </summary>
    /// <param name="id">Object identifier.</param>
    abstract GetById : id:string -> Async<ICloudValue>

    /// <summary>
    ///     Gets all cloud value references contained in instance.
    /// </summary>
    abstract GetAllValues : unit -> Async<ICloudValue []>

    /// <summary>
    ///     Asynchronously disposes value from caching context.
    /// </summary>
    /// <param name="container">CloudValue container.</param>
    abstract Dispose : value:ICloudValue -> Async<unit>

    /// <summary>
    ///     Asynchronously disposes all values from caching context.
    /// </summary>
    abstract DisposeAllValues : unit -> Async<unit>

namespace MBrace.Core

open MBrace.Core.Internals

type CloudValue =

    /// Gets the default cache storage level used by the runtime.
    static member DefaultStorageLevel = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return provider.DefaultStorageLevel
    }

    /// <summary>
    ///     Checks if provided storage level is supported by the current
    ///     CloudValue implementation.
    /// </summary>
    /// <param name="storageLevel">Storage level to be checked.</param>
    static member IsSupportedStorageLevel (storageLevel : StorageLevel) : Local<bool> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return provider.IsSupportedStorageLevel storageLevel    
    }
    
    /// <summary>
    ///     Creates a new CloudValue instance with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel to be used for CloudValue.</param>
    static member New<'T>(value : 'T, ?storageLevel : StorageLevel) : Local<ICloudValue<'T>> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        let storageLevel = defaultArg storageLevel provider.DefaultStorageLevel
        return! provider.CreateCloudValue(value, storageLevel)
    }

    /// <summary>
    ///     Creates a partitioned set of CloudArrays from input sequence according to size.
    /// </summary>
    /// <param name="values">Input set of values.</param>
    /// <param name="storageLevel">StorageLevel to be used for CloudValues.</param>
    /// <param name="partitionThreshold">Partition threshold in bytes. Defaults to infinite threshold.</param>
    static member NewPartitioned<'T>(values : seq<'T>, ?storageLevel : StorageLevel, ?partitionThreshold : int64) : Local<ICloudArray<'T> []> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        let storageLevel = defaultArg storageLevel provider.DefaultStorageLevel
        return! provider.CreatePartitionedArray(values, storageLevel, ?partitionThreshold = partitionThreshold)
    }

    /// <summary>
    ///     Dereferences a cloud value.
    /// </summary>
    /// <param name="value">CloudValue instance.</param>
    static member Read(value : ICloudValue<'T>) : Local<'T> = local {
        return! value.GetValueAsync()
    }

    /// <summary>
    ///     Deletes the provided CloudValue from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    static member Delete (value : ICloudValue) : Local<unit> = local {
        return! value.Dispose()
    }