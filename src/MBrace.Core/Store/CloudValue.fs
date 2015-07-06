namespace MBrace.Core

#nowarn "444"

open System

/// Storage levels used for caching
type StorageLevel =
    | MemoryOnly = 1
    | MemoryAndDisk = 2
    | MemorySerialized = 4
    | MemoryAndDiskSerialized = 8
    | DiskOnly = 16

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
    abstract ValueBoxed : obj
    /// Asynchronously gets the boxed payload of the CloudValue.
    abstract GetBoxedValueAsync : unit -> Async<obj>

/// Serializable entity that represents an immutable 
/// .NET object that has been cached by the MBrace runtime.
type ICloudValue<'T> =
    inherit ICloudValue
    /// Gets the payload of the CloudValue.
    abstract Value : 'T
    /// Asynchronously gets the boxed payload of the CloudValue.
    abstract GetValueAsync : unit -> Async<'T>

namespace MBrace.Core.Internals

open MBrace.Core

/// Cloud Value provider implementation.
type ICloudValueProvider =

    /// Implementation name
    abstract Name : string

    /// CloudValue implementation instance identifier
    abstract Id : string

    /// <summary>
    ///     Initializes a CloudValue with supplied payload.
    /// </summary>
    /// <param name="payload">Payload to be cached.</param>
    abstract CreateCloudValue : payload:'T -> Async<ICloudValue<'T>>

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
    
    /// <summary>
    ///     Creates a new CloudValue instance with provided payload.
    /// </summary>
    /// <param name="value">Payload for CloudValue.</param>
    static member New<'T>(value : 'T) : Local<ICloudValue<'T>> = local {
        let! provider = Cloud.GetResource<ICloudValueProvider> ()
        return! provider.CreateCloudValue value
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
    static member Delete (value : ICloudValue) : Local<unit> = dispose value