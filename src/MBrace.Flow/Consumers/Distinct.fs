namespace MBrace.Flow.Internals.Consumers

open System
open System.IO
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

module Distinct =

    /// <summary>Returns a flow that contains no duplicate entries according to the generic hash and equality comparisons on the keys returned by the given key-generating function. If an element occurs multiple times in the flow then only one is retained.</summary>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <param name="source">The input flow.</param>
    /// <returns>A flow of elements distinct on their keys.</returns>
    let distinctBy (projection : 'T -> 'Key) (source : CloudFlow<'T>) : CloudFlow<'T> =
        // Stage 1: distinct and shuffle data to cluster
        let distinctAndShuffle () = cloud {
            let distinctCollectorF = local {
                let dict = new ConcurrentDictionary<'Key, 'T>()
                let! ct = Cloud.CancellationToken
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return
                    { new Collector<'T, ICollection<'Key * 'T>> with
                        member __.DegreeOfParallelism = source.DegreeOfParallelism
                        member __.Iterator() =
                            {   Func = fun v -> dict.TryAdd(projection v, v) |> ignore
                                Cts = cts }
                        member __.Result = dict |> Collection.map (fun kv -> kv.Key, kv.Value)
                    }
            }

            let! workers = Cloud.GetAvailableWorkers()
            let workers = workers |> Array.sortBy (fun w -> w.Id)
            let workers = 
                match source.DegreeOfParallelism with 
                | None -> workers
                | Some dp -> Array.init (min dp workers.Length) (fun i -> workers.[i % workers.Length])
            
            // local shuffle of grouped results
            let shuffleLocal (groupings : seq<'Key * 'T>) = local {
                return!
                    groupings
                    |> Seq.groupBy (fun (k,_) -> workers.[abs (hash k) % workers.Length])
                    |> Seq.map (fun (w,gps) -> local { let! ca = CloudValue.NewArray(gps |> Seq.map snd, storageLevel = StorageLevel.Disk) in return w, ca })
                    |> Local.Parallel
            }

            // top-level shuffled results combiner
            let combiner (gathered : (IWorkerRef * CloudArray<'T>) [] []) = local {
                return gathered |> Seq.concat |> Seq.sortBy fst |> Seq.toArray
            }

            let! shuffleResults = source.WithEvaluators distinctCollectorF shuffleLocal combiner
            return PersistedCloudFlow<'T>(shuffleResults) :> CloudFlow<'T>
        }

        // Stage 2 : Perform final distinction operation on shuffled data
        let reduceFlow (collectorf : LocalCloud<Collector<'T, 'S>>) 
                        (flowProjection : 'S -> LocalCloud<'R>) (flowCombiner : 'R [] -> LocalCloud<'R>)
                        (shuffled : CloudFlow<'T>) = cloud {

            let reduceCollectorF = local {
                let! ct = Cloud.CancellationToken
                let dict = new ConcurrentDictionary<'Key, 'T>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return 
                    { new Collector<'T, ICollection<'T>> with
                        member self.DegreeOfParallelism = source.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func = fun t -> dict.TryAdd(projection t, t) |> ignore
                                Cts = cts }

                        member self.Result = dict |> Collection.map (fun kv -> kv.Value)
                    }
            }

            let reduceProjection (inputs : ICollection<'T>) = local {
                let! collector = collectorf
                let iterators = Array.init Environment.ProcessorCount (fun _ -> collector.Iterator())
                let! _ =
                    inputs
                    |> Collection.splitByPartitionCount iterators.Length
                    |> Seq.mapi (fun i ts -> local { let iter = iterators.[i] in return for t in ts do iter.Func t })
                    |> Local.Parallel

                return! flowProjection collector.Result
            }

            return! shuffled.WithEvaluators reduceCollectorF reduceProjection flowCombiner
        }

        { new CloudFlow<'T> with
            member __.DegreeOfParallelism = source.DegreeOfParallelism
            member __.WithEvaluators<'S, 'R> collectorF (projection : 'S -> LocalCloud<'R>) combiner = cloud {
                let! shuffledResult = distinctAndShuffle()
                return! reduceFlow collectorF projection combiner shuffledResult
            }
        }