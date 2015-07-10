namespace MBrace.Store

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

/// Partitionable implementation of HTTP file line reader
[<DataContract>]
type HTTPTextLineSequence(url : string, ?encoding : Encoding, ?deserializer : (Stream -> seq<string>)) =
    
    let getSize = local {
            use stream = new PartialHTTPStream(url)
            return stream.Length
    }

    let toEnumerable = local {
            let stream = new PartialHTTPStream(url)
            match deserializer with
            | Some deserializer -> return deserializer stream
            | None -> return TextReaders.ReadLines(stream, ?encoding = encoding)
    }

    interface ICloudCollection<string> with
        member c.IsKnownCount = false
        member c.IsKnownSize = true
        member c.Count = raise <| new NotSupportedException()
        member c.Size = getSize
        member c.ToEnumerable() = toEnumerable

    interface IPartitionableCollection<string> with
        member cs.GetPartitions(weights : int []) = local {
            let! size = getSize

            let mkRangedSeqs (weights : int[]) =
                let getDeserializer s e stream = TextReaders.ReadLinesRanged(stream, max (s - 1L) 0L, e, ?encoding = encoding)
                let mkRangedSeq rangeOpt =
                    match rangeOpt with
                    | Some(s,e) -> 
                        let deserializer = Some (getDeserializer s e)
                        new HTTPTextLineSequence(url, ?encoding = encoding, ?deserializer = deserializer) :> ICloudCollection<string>
                    | None -> new SequenceCollection<string>([||]) :> _

                let partitions = Array.splitWeightedRange weights 0L size
                Array.map mkRangedSeq partitions

            if size < 512L * 1024L then
                let! lines = toEnumerable 
                let liness = Array.splitWeighted weights (lines |> Seq.toArray)
                return liness |> Array.map (fun lines -> new SequenceCollection<string>(lines) :> ICloudCollection<_>)
                
            else
                return mkRangedSeqs weights
        }

    


