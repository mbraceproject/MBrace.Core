namespace MBrace.Flow

open System
open Nessos.Streams

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Library.Internals

open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

type internal CloudCollection private () =

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    /// <param name="weight">Worker weighing function. Defaults to processor count.</param>
    /// <param name="sizeThresholdPerWorker">Restricts concurrent processing of collection partitions up to specified size per worker.</param>
    static member ToCloudFlow (collection : ICloudCollection<'T>, ?weight : IWorkerRef -> int, ?sizeThresholdPerWorker:unit -> int64) : CloudFlow<'T> =
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

                // have partitions, schedule according to number of partitions.
                let createTask (partitions : ICloudCollection<'T> []) = local {
                    // further partition according to collection size threshold, if so specified.
                    let! partitionSlices = local {
                        match sizeThresholdPerWorker with
                        | None -> return [| partitions |]
                        | Some f -> return! partitions |> Partition.partitionBySize (fun p -> p.Size) (f ())
                    }

                    // compute a single partition
                    let computePartitionSlice (slice : ICloudCollection<'T> []) = local {
                        let! collector = collectorf
                        let! seqs = slice |> Seq.map (fun p -> p.ToEnumerable()) |> Async.Parallel
                        let pStream = seqs |> ParStream.ofArray |> ParStream.collect Stream.ofSeq
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