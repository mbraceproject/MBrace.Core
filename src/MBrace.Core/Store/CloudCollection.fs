namespace MBrace.Core.Internals

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Core

#nowarn "444"

/// Represents an abstract, distributed collection of values.
type ICloudCollection<'T> =
    inherit seq<'T>
    /// Returns true if size property is cheap to compute.
    /// Collections that require traversal to determine size
    /// should return false.
    abstract IsKnownSize : bool
    /// Returns true if count property is cheap to compute.
    /// Collections that require traversal to determine count
    /// should return false.
    abstract IsKnownCount : bool
    /// Returns true if collection exists in-memory without the need to perform IO.
    abstract IsMaterialized : bool
    /// Gets a size metric for the collection.
    /// This could be total amount of bytes of persisting files
    /// or the total number of elements if this is a known value.
    /// It used for weighing collection partitions.
    abstract GetSizeAsync : unit -> Async<int64>
    /// Computes the element count for the collection.
    abstract GetCountAsync : unit -> Async<int64>
    /// Gets an enumeration of all elements in the collection
    abstract GetEnumerableAsync : unit -> Async<seq<'T>>

/// A cloud collection that comprises of a fixed number of partitions.
type IPartitionedCollection<'T> =
    inherit ICloudCollection<'T>
    /// Gets the partition count of the collection.
    abstract PartitionCount : Async<int>
    /// Gets all partitions for the collection.
    abstract GetPartitions : unit -> Async<ICloudCollection<'T> []>

/// A partitioned cloud collection whose partitions target a specified worker.
type ITargetedPartitionCollection<'T> =
    inherit IPartitionedCollection<'T>
    /// Gets all partitions for the collection, as targeted to specified workers.
    abstract GetTargetedPartitions : unit -> Async<(IWorkerRef * ICloudCollection<'T>) []>

/// A cloud collection that can be partitioned into smaller collections of provided size.
type IPartitionableCollection<'T> =
    inherit ICloudCollection<'T>
    /// Partitions the collection into collections of given count
    abstract GetPartitions : weights:int[] -> Async<ICloudCollection<'T> []>