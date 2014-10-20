namespace Nessos.MBrace

open System
open System.IO
open System.Collections.Generic

/// Represents an entity that resides in the runtime's storage implementation
type IStorageEntity =
    inherit ICloudDisposable
    /// Unique store identifier.
    abstract Uri : string
    /// Container (directory) name
    abstract Container : string
    /// File name
    abstract Name : string

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references are cached locally for performance.
type ICloudRef<'T> =
    inherit IStorageEntity
    /// Asynchronously dereferences the cloud ref.
    abstract GetValue : unit -> Async<'T>

/// Represents a finite and immutable sequence of
/// elements that is persisted in the underlying store
/// and can be enumerated on demand.
type ICloudSeq<'T> =
    inherit IStorageEntity
    inherit IEnumerable<'T>

    /// Approximate size (in bytes) of the referenced CloudSeq.
    abstract Size : int64
    /// CloudSeq element count.
    abstract Count : int

/// Represents a mutable reference to an
/// object that is persisted in the underlying store.
type IMutableCloudRef<'T> = 
    inherit IStorageEntity

    /// Asynchronously dereferences current value of MutableCloudRef.
    abstract GetValue : unit -> Async<'T>
    /// Asynchronously attempts to update the MutableCloudRef; returns true if successful.
    abstract TryUpdate : 'T -> Async<bool>
    /// Asynchronously forces update to MutableCloudRef regardless of state.
    abstract ForceUpdate : 'T -> Async<unit>

/// Represents a binary file persisted in the underlying store.
type ICloudFile =
    inherit IStorageEntity

    /// CloudFile size in bytes.
    abstract Size : int64
    /// Asynchronously returns a reader stream to the file data.
    abstract Read : unit -> Async<Stream>


/// Represents a finite and immutable sequence of
/// elements that is persisted in the underlying store
/// and provides fast random access.
type ICloudArray<'T> =
    inherit IStorageEntity
    inherit IEnumerable<'T>

    /// The number of elements contained.
    abstract Count : int64 
    /// Approximate collection size in bytes.
    abstract Size : byte []
        
    /// <summary>
    ///     Combines two CloudArrays into one.
    /// </summary>
    /// <param name="other">Other cloud array to be joined.</param>
    abstract Append : other : ICloudArray<'T> -> ICloudArray<'T>

    /// <summary>
    ///     Returns the item in the specified index.
    /// </summary>
    /// <param name="index">The item's index.</param>
    abstract Item : index : int64 -> 'T with get

    /// <summary>
    ///     Returns an array of the elements in the specified range.
    /// </summary>
    /// <param name="start">The starting index.</param>
    /// <param name="count">The number of elements to return.</param>
    abstract Range : start : int64 * count : int -> 'T []