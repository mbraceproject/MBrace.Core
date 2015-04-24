namespace MBrace.Store.Internals

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
    /// Gets a size metric for the collection.
    /// This could be total amount of bytes of persisting files
    /// or the total number of elements if this is a known value.
    /// It used for weighing collection partitions.
    abstract Size : Local<int64>
    /// Returns true if size property is cheap to compute.
    /// Collections that require traversal to determine size
    /// should return false.
    abstract IsKnownSize : bool
    /// Computes the element count for the collection.
    abstract Count : Local<int64>
    /// Returns true if count property is cheap to compute.
    /// Collections that require traversal to determine count
    /// should return false.
    abstract IsKnownCount : bool
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


[<AutoOpen>]
module private SequenceImpl =

    let getCount (seq : seq<'T>) = 
        match seq with
        | :? ('T list) as ts -> ts.Length
        | :? ICollection<'T> as c -> c.Count
        | _ -> Seq.length seq
        |> int64

    let isKnownCount (seq : seq<'T>) =
        match seq with
        | :? ('T list)
        | :? ICollection<'T> -> true
        | _ -> false

/// ICloudCollection wrapper for serializable IEnumerables
[<Sealed; DataContract>]
type SequenceCollection<'T> (seq : seq<'T>) =
    [<DataMember(Name = "Sequence")>]
    let seq = seq
    member __.Sequence = seq
    interface ICloudCollection<'T> with
        member x.IsKnownSize = isKnownCount seq
        member x.IsKnownCount = isKnownCount seq
        member x.Count: Local<int64> = local { return getCount seq }
        member x.Size: Local<int64> = local { return getCount seq }
        member x.ToEnumerable(): Local<seq<'T>> = local { return seq }

/// Partitionable ICloudCollection wrapper for a collection of serializable IEnumerables
[<Sealed; DataContract>]
type PartitionedSequenceCollection<'T> (sequences : seq<'T> []) =
    [<DataMember(Name = "Sequences")>]
    let sequences = sequences
    static let mkCollection (seq : seq<'T>) = new SequenceCollection<'T>(seq) :> ICloudCollection<'T>
    /// Gets all sequence partitions.
    member __.Sequences = sequences
    interface IPartitionedCollection<'T> with
        member x.IsKnownSize = sequences |> Array.forall isKnownCount
        member x.IsKnownCount = sequences |> Array.forall isKnownCount
        member x.Count: Local<int64> = local { return sequences |> Array.sumBy getCount }
        member x.Size: Local<int64> = local { return sequences |> Array.sumBy getCount }
        member x.GetPartitions(): Local<ICloudCollection<'T> []> = local { return sequences |> Array.map mkCollection }
        member x.PartitionCount: Local<int> = local { return sequences.Length }
        member x.ToEnumerable(): Local<seq<'T>> = local { return Seq.concat sequences }