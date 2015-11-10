namespace MBrace.Core

#nowarn "444"

open System
open System.Diagnostics
open System.Runtime.CompilerServices
open System.ComponentModel

open MBrace.Core.Internals

/// Storage levels used for CloudValues
type StorageLevel =
    /// Data persisted to disk only.
    /// Has to be fetched on every value request.
    | Disk                      = 1

    /// Data cached in-memory as materialized object.
    /// Faster access but mutation-sensitive.
    | Memory                    = 2

    /// Data cached in-memory as pickled byte array.
    /// Slower access but dense representation.
    | MemorySerialized          = 4

    /// Data small enough to be encapsulated by reference object.
    /// Used by storage implementations as an optimization.
    | Encapsulated              = 8

    /// Data is managed by a resource independent of the caching implementation.
    /// For instance, it could be a Vagabond data dependency.
    | Other                     = 16

    // Enumerations combining multiple storage levels

    /// Data persisted to disk and cached in-memory as materialized object.
    | MemoryAndDisk             = 3

    /// Data persisted to disk and cached in-memory as pickled byte array.
    | MemoryAndDiskSerialized   = 5

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
    /// Reflected type of value referenced by CloudValue.
    abstract ReflectedType : Type
    /// Determines if cached value already exists
    /// in the local execution context.
    abstract IsCachedLocally : bool
    /// Gets the boxed payload of the CloudValue.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract ValueBoxed : obj
    /// Asynchronously gets the boxed payload of the CloudValue.
    abstract GetValueBoxedAsync : unit -> Async<obj>
    /// Casts CloudValue to specified type, if applicable.
    abstract Cast<'S> : unit -> CloudValue<'S>

/// Serializable entity that represents an immutable 
/// .NET object that has been cached by the MBrace runtime.
and CloudValue<'T> =
    inherit ICloudValue
    /// Gets the payload of the CloudValue.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract Value : 'T
    /// Asynchronously gets the payload of the CloudValue.
    abstract GetValueAsync : unit -> Async<'T>

/// Serializable entity that represents an immutable 
/// array that has been cached by the MBrace runtime.
type CloudArray<'T> =
    inherit seq<'T>
    inherit CloudValue<'T []>
    inherit ICloudCollection<'T>
    /// Array element count
    abstract Length : int

[<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
type CloudValueExtensions =

    /// Asynchronously gets the boxed payload of the CloudValue.
    [<Extension>]
    static member GetValueBoxed(this : ICloudValue) = Async.RunSync <| this.GetValueBoxedAsync()
    /// Asynchronously gets the payload of the CloudValue.
    [<Extension>]
    static member GetValue(this : CloudValue<'T>) = Async.RunSync <| this.GetValueAsync()
    

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
    ///     Generates a structural CloudValue identifier for given value,
    ///     without creating a new CloudValue instance.
    /// </summary>
    /// <param name="value">Value to used to structurally extract an identifier.</param>
    abstract GetCloudValueId : value:'T -> string

    /// <summary>
    ///     Initializes a CloudValue with supplied payload.
    /// </summary>
    /// <param name="payload">Payload to be cached.</param>
    /// <param name="storageLevel">Storage level for cloud value.</param>
    abstract CreateCloudValue : payload:'T * storageLevel:StorageLevel -> Async<CloudValue<'T>>

    /// <summary>
    ///     Creates a sequence partitioning implementation that splits inputs into chunks
    ///     according to size in bytes.
    /// </summary>
    /// <param name="sequence">Input sequence to be partitioned.</param>
    /// <param name="partitionThreshold">Maximum partition size in bytes per chunk.</param>
    /// <param name="storageLevel">Storage level for cloud arrays.</param>
    abstract CreateCloudArrayPartitioned : sequence:seq<'T> * partitionThreshold:int64 * storageLevel:StorageLevel -> Async<CloudArray<'T> []>

    /// <summary>
    ///     Try getting a created CloudValue by supplied id.
    /// </summary>
    /// <param name="id">Cloud value identifier.</param>
    abstract TryGetCloudValueById : id:string -> Async<ICloudValue option>

    /// <summary>
    ///     Gets all cloud value references defined in instance.
    /// </summary>
    abstract GetAllCloudValues : unit -> Async<ICloudValue []>

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
    static member IsSupportedStorageLevel (storageLevel : StorageLevel) : LocalCloud<bool> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return provider.IsSupportedStorageLevel storageLevel    
    }
    
    /// <summary>
    ///     Creates a new CloudValue instance with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    /// <param name="storageLevel">StorageLevel to be used for CloudValue.</param>
    static member New<'T>(value : 'T, ?storageLevel : StorageLevel) : LocalCloud<CloudValue<'T>> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        let storageLevel = defaultArg storageLevel provider.DefaultStorageLevel
        return! Cloud.OfAsync <| provider.CreateCloudValue(value, storageLevel)
    }

    /// <summary>
    ///     Creates a partitioned set of CloudArrays from input sequence according to size.
    /// </summary>
    /// <param name="values">Input set of values.</param>
    /// <param name="storageLevel">StorageLevel to be used for CloudValues.</param>
    static member NewArray<'T>(values : seq<'T>, ?storageLevel : StorageLevel) : LocalCloud<CloudArray<'T>> = local {
        let! cval = CloudValue.New(Seq.toArray values, ?storageLevel = storageLevel)
        return cval :?> CloudArray<'T>
    }

    /// <summary>
    ///     Creates a partitioned set of CloudArrays from input sequence according to size.
    /// </summary>
    /// <param name="values">Input set of values.</param>
    /// <param name="partitionThreshold">Partition threshold in bytes.</param>
    /// <param name="storageLevel">StorageLevel to be used for CloudValues.</param>
    static member NewArrayPartitioned<'T>(values : seq<'T>, partitionThreshold : int64, ?storageLevel : StorageLevel) : LocalCloud<CloudArray<'T> []> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        let storageLevel = defaultArg storageLevel provider.DefaultStorageLevel
        return! Cloud.OfAsync <| provider.CreateCloudArrayPartitioned(values, partitionThreshold, storageLevel)
    }

    /// <summary>
    ///     Casts given CloudValue instance to specified type.
    /// </summary>
    /// <param name="cloudValue">CloudValue instance to be cast.</param>
    static member Cast<'T>(cloudValue : ICloudValue) : CloudValue<'T> =
        cloudValue.Cast<'T> ()

    /// <summary>
    ///     Retrieves a CloudValue instance by provided id.
    /// </summary>
    /// <param name="id">CloudValue identifier.</param>
    static member TryGetValueById(id : string) : LocalCloud<ICloudValue option> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return! Cloud.OfAsync <| provider.TryGetCloudValueById(id)
    }

    /// <summary>
    ///     Fetches all existing CloudValue from underlying store.
    /// </summary>
    static member GetAllValues() : LocalCloud<ICloudValue []> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return! Cloud.OfAsync <| provider.GetAllCloudValues()
    }

    /// <summary>
    ///     Dereferences a cloud value.
    /// </summary>
    /// <param name="value">CloudValue instance.</param>
    static member Read(value : CloudValue<'T>) : LocalCloud<'T> = local {
        return! Cloud.OfAsync <| value.GetValueAsync()
    }

    /// <summary>
    ///     Deletes the provided CloudValue from store.
    /// </summary>
    /// <param name="atom">Atom instance to be deleted.</param>
    static member Delete (value : ICloudValue) : LocalCloud<unit> = local {
        return! Cloud.OfAsync <| value.Dispose()
    }