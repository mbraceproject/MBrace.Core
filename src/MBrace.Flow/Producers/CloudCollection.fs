namespace MBrace.Flow

open System
open Nessos.Streams

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Workflows

open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

type internal CloudCollection private () =

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    /// <param name="useCache">Make use of caching, if the collection supports it. Defaults to false.</param>
    /// <param name="weight">Worker weighing function. Defaults to processor count.</param>
    /// <param name="sizeThresholdPerWorker">Restricts concurrent processing of collection partitions up to specified size per worker.</param>
    static member ToCloudFlow (collection : ICloudCollection<'T>, ?useCache:bool, ?weight : IWorkerRef -> int, ?sizeThresholdPerWorker:unit -> int64) : CloudFlow<'T> =
        let useCache = defaultArg useCache false
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.WithEvaluators<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! collector = collectorf
                let! isTargetedWorkerSupported = Cloud.IsTargetedWorkerSupported
                let! workers = Cloud.GetAvailableWorkers()
                // sort by id to ensure deterministic scheduling
                let workers = workers |> Array.sortBy (fun w -> w.Id)
                let workers =
                    match collector.DegreeOfParallelism with
                    | None -> workers
                    | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]

                let! partitions = CloudCollection.ExtractPartitions collection
                let! partitionss = CloudCollection.PartitionBySize(partitions, workers, isTargetedWorkerSupported, ?weight = weight)
                if Array.isEmpty partitionss then return! combiner [||] else

                // use caching, if supported by collection
                let tryGetCachedContents (collection : ICloudCollection<'T>) = local {
                    match box collection with
                    | :? ICloudCacheable<'T []> as cc -> 
                        if useCache then 
                            let! result = CloudCache.GetCachedValue(cc, cacheIfNotExists = true)
                            return Some result
                        else 
                            return! CloudCache.TryGetCachedValue cc

                    | _ -> return None
                }

                // have partitions, schedule according to number of partitions.
                let createTask (partitions : ICloudCollection<'T> []) = local {
                    // further partition according to collection size threshold, if so specified.
                    let! partitionSlices =
                        match sizeThresholdPerWorker with
                        | None -> local { return [| partitions |] }
                        | Some f -> partitions |> Partition.partitionBySize (fun p -> p.Size) (f ())

                    // compute a single partition
                    let computePartitionSlice (slice : ICloudCollection<'T> []) = local {
                        let getSeq (p : ICloudCollection<'T>) = local {
                            let! cached = tryGetCachedContents p
                            match cached with
                            | Some c -> return c :> seq<'T>
                            | None -> return! p.ToEnumerable()
                        }

                        let! collector = collectorf
                        let! seqs = Local.Sequential.map getSeq slice
                        let pStream = seqs |> Seq.concat |> ParStream.ofSeq
                        let value = pStream.Apply (collector.ToParStreamCollector())
                        return! projection value
                    }

                    // sequentially compute partitions
                    let! results = Local.Sequential.map computePartitionSlice partitionSlices
                    return! combiner results
                }
                        
                let! results =
                    if isTargetedWorkerSupported then
                        partitionss
                        |> Seq.filter (not << Array.isEmpty << snd)
                        |> Seq.map (fun (w,partitions) -> createTask partitions, w)
                        |> Cloud.Parallel
                    else
                        partitionss
                        |> Seq.filter (not << Array.isEmpty << snd)
                        |> Seq.map (createTask << snd)
                        |> Cloud.Parallel

                return! combiner results
            }
        }