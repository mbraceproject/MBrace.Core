namespace MBrace.Store

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Represents an abstract, distributed collection of values.
type ICloudCollection<'T> =
    /// Computes the element count for the collection.
    abstract Count : Local<int64>
    /// Gets a size metric for the collection.
    /// This could be total amount of bytes of persisting files
    /// or the total number of elements if this is a known value.
    /// Importantly this should be cheap to compute, not requiring
    /// traversal of the entire collection. It used for weighing collection partitions.
    abstract Size : Local<int64>
    /// Gets an enumeration of all elements in the collection
    abstract ToEnumerable : unit -> Local<seq<'T>>

/// A cloud collection that comprises of a fixed number of partitions.
type IPartitionedCollection<'T> =
    inherit ICloudCollection<'T>
    /// Gets the partition count of the collection.
    abstract PartitionCount : Local<int>
    /// Gets all partitions for the collection.
    abstract GetPartitions : unit -> Local<ICloudCollection<'T> []>

/// A cloud collection that can be partitioned into smaller collections of provided size.
type IPartitionableCollection<'T> =
    inherit ICloudCollection<'T>
    /// Partitions the collection into collections of given count
    abstract GetPartitions : partitionCount:int -> Local<ICloudCollection<'T> []>