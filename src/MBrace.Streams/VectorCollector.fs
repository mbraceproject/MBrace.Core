namespace MBrace.Streams.Internals

open MBrace
open MBrace.Streams
open MBrace.Workflows
open System.Collections.Generic
    
/// [omit] For internal use only. CloudVector with in-memory cachemap and non-monadic merge.
type VectorCollector<'T> (count, partitions, cacheMap) =
    member val Count = count
    member val Partitions = partitions
    member val internal CacheMap = cacheMap

    member __.ToCloudVector () = 
        cloud {
            let! map = CloudAtom.New(cacheMap)
            return new CloudVector<'T>(count, partitions, map)
        }

    member __.ToEnumerable () : Cloud<IEnumerable<'T>> =
        cloud {
            return! partitions |> Sequential.lazyCollect (fun p -> p.ToEnumerable())
        }

    member v1.Merge(v2 : VectorCollector<'T>) =
        let count = v1.Count + v2.Count
        let partitions = Array.append v1.Partitions v2.Partitions
        let cache = CacheState.Combine(v1.CacheMap, v2.CacheMap)
        new VectorCollector<'T>(count, partitions, cache)

    static member New(values : seq<'T>, maxPartitionSize : int64) =
        cloud {
            let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize)
            let count = ref 0L
            for p in partitions do 
                let! c = p.Count
                count := !count + int64 c
            let! context = Cloud.GetSchedulingContext()
            let! map  = 
                match context with
                | ThreadParallel
                | Sequential -> cloud.Return None
                | Distributed -> cloud { 
                    let! w = Cloud.CurrentWorker
                    return (w, partitions)
                            |> Seq.singleton
                            |> Map.ofSeq :> IDictionary<_, _>
                            |> Some 
                }
            return new VectorCollector<'T>(count.Value, partitions, map)
        }
