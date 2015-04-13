namespace MBrace.Flow

open System
open Nessos.Streams

open MBrace
open MBrace.Store
open MBrace.Continuation
open MBrace.Workflows

open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

type internal CloudCollection =

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    /// <param name="useCache">Make use of caching, if the collection supports it. Defaults to false.</param>
    /// <param name="sizeThresholdPerWorker">Restricts concurrent processing of collection partitions up to specified size per worker.</param>
    static member ToCloudFlow (collection : ICloudCollection<'T>, ?useCache:bool, ?sizeThresholdPerWorker:unit -> int64) : CloudFlow<'T> =
        let useCache = defaultArg useCache false
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    // TODO: 
                    //   1. take nested partitioning into account
                    //   2. partition according to collection sizes; current partition schemes assume that inputs are homogeneous
                    //      but this might not be the case.
                    let! collector = collectorf
                    let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                    let! workers = Cloud.GetAvailableWorkers()
                    // sort by id to ensure deterministic scheduling
                    let workers = workers |> Array.sortBy (fun w -> w.Id)
                    let workers =
                        match collector.DegreeOfParallelism with
                        | None -> workers
                        | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]

                    // detect partitions for collection, if any
                    let! partitions = local {
                        match collection with
                        | :? IPartitionedCollection<'T> as pcc -> let! partitions = pcc.GetPartitions() in return Some partitions
                        | :? IPartitionableCollection<'T> as pcc ->
                            // got a collection that can be partitioned dynamically;
                            // partition according core counts per worker
                            let partitionCount = workers |> Array.map (fun w -> w.ProcessorCount) |> Array.gcdNormalize |> Array.sum
                            let! partitions = pcc.GetPartitions partitionCount 
                            return Some partitions

                        | _ -> return None
                    }

                    // use caching, if supported by collection
                    let tryGetCachedContents (collection : ICloudCollection<'T>) = local {
                        match box collection with
                        | :? Store.ICloudCacheable<'T []> as cc -> 
                            if useCache then 
                                let! result = CloudCache.GetCachedValue(cc, cacheIfNotExists = true)
                                return Some result
                            else 
                                return! CloudCache.TryGetCachedValue cc

                        | _ -> return None
                    }

                    match partitions with
                    | None ->
                        // no partition scheme, materialize collection in-memory and offload to CloudFlow.ofArray
                        let! cached = tryGetCachedContents collection
                        let! array = local {
                            match cached with
                            | Some c -> return c
                            | None ->
                                let! enum = collection.ToEnumerable()
                                return Seq.toArray enum
                        }

                        let arrayFlow = Array.ToCloudFlow array
                        return! arrayFlow.Apply collectorf projection combiner

                    | Some [||] -> return! combiner [||]
                    | Some partitions ->
                        // have partitions, schedule according to number of partitions.
                        let createTask (partitions : ICloudCollection<'T> []) = local {
                            // further partition according to collection size threshold, if so specified.
                            let! partitionss =
                                match sizeThresholdPerWorker with
                                | None -> local { return [| partitions |] }
                                | Some f -> partitions |> Partition.partitionBySize (fun p -> p.Size) (f ())

                            // compute a single partition
                            let computePartition (partition : ICloudCollection<'T> []) = local {
                                let getSeq (p : ICloudCollection<'T>) = local {
                                    let! cached = tryGetCachedContents p
                                    match cached with
                                    | Some c -> return c :> seq<'T>
                                    | None -> return! p.ToEnumerable()
                                }

                                let! collector = collectorf
                                let! seqs = Sequential.map getSeq partitions
                                let pStream = seqs |> ParStream.ofArray |> ParStream.collect Stream.ofSeq
                                let value = pStream.Apply (collector.ToParStreamCollector())
                                return! projection value
                            }

                            // sequentially compute partitions
                            let! results = Sequential.map computePartition partitionss
                            return! combiner results
                        }
                        
                        let! results =
                            if targetedworkerSupport then
                                partitions
                                |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (fun (w,partitions) -> createTask partitions, w)
                                |> Cloud.Parallel
                            else
                                partitions
                                |> WorkerRef.partition workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (createTask << snd)
                                |> Cloud.Parallel

                        return! combiner results
                }
        }