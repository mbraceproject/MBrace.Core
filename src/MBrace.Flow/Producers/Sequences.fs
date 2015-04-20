namespace MBrace.Flow

open System.Collections.Generic
open System.Runtime.Serialization

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

open MBrace.Flow
open MBrace.Flow.Internals

[<AutoOpen>]
module private SequenceImpl =

    // Implements a set of serializable enumerables as a partitioned cloud collection

    let getCount (seq : seq<'T>) = 
        match seq with
        | :? ('T list) as ts -> ts.Length
        | :? ICollection<'T> as c -> c.Count
        | _ -> Seq.length seq
        |> int64

    [<Sealed; DataContract>]
    type SingularSequenceCollection<'T> (seq : seq<'T>) =
        [<DataMember(Name = "Sequence")>]
        let seq = seq
        interface ICloudCollection<'T> with
            member x.Count: Local<int64> = local { return getCount seq }
            member x.Size: Local<int64> = local { return getCount seq }
            member x.ToEnumerable(): Local<seq<'T>> = local { return seq }

    [<Sealed; DataContract>]
    type SequenceCollection<'T> (seqs : seq<'T> []) =
        [<DataMember(Name = "Sequences")>]
        let seqs = seqs
        static let mkCollection (seq : seq<'T>) = new SingularSequenceCollection<'T>(seq) :> ICloudCollection<'T>
        interface IPartitionedCollection<'T> with
            member x.Count: Local<int64> = local { return seqs |> Array.sumBy getCount }
            member x.Size: Local<int64> = local { return seqs |> Array.sumBy getCount }
            member x.GetPartitions(): Local<ICloudCollection<'T> []> = local { return seqs |> Array.map mkCollection }
            member x.PartitionCount: Local<int> = local { return seqs.Length }
            member x.ToEnumerable(): Local<seq<'T>> = local { return Seq.concat seqs }

type internal Sequences =

    /// <summary>
    ///     Creates a CloudFlow instance from a finite collection of serializable enumerations.
    /// </summary>
    /// <param name="enumerations">Input enumerations.</param>
    static member OfSeqs (enumerations : seq<#seq<'T>>) : CloudFlow<'T> =
        let enumerations = enumerations |> Seq.map (fun s -> s :> seq<'T>) |> Seq.toArray
        let collection = new SequenceCollection<'T>(enumerations)
        CloudCollection.ToCloudFlow collection