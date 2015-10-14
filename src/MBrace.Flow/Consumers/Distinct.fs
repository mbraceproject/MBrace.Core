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

#nowarn "444"

module Distinct =

    /// <summary>Returns a flow that contains no duplicate entries according to the generic hash and equality comparisons on the keys returned by the given key-generating function. If an element occurs multiple times in the flow then only one is retained.</summary>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <param name="source">The input flow.</param>
    /// <returns>A flow of elements distinct on their keys.</returns>
    let distinctBy (projection : 'T -> 'Key) (source : CloudFlow<'T>) : CloudFlow<'T> =
        let collectorF (cloudCt : ICloudCancellationToken) (totalWorkers : int) =
            local {
                let dict = new ConcurrentDictionary<'Key, 'T>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
                return
                    { new Collector<'T, seq<int * seq<'Key * 'T>>> with
                      member __.DegreeOfParallelism = source.DegreeOfParallelism
                      member __.Iterator() =
                          { Index = ref -1;
                            Func = (fun v -> let k = projection v in dict.TryAdd(k, v) |> ignore);
                            Cts = cts }
                      member __.Result =
                           dict
                           |> Seq.groupBy (fun kv -> Math.Abs(kv.Key.GetHashCode()) % totalWorkers)
                           |> Seq.map (fun (pk, kvs) -> (pk, kvs |> Seq.map (fun kv -> (kv.Key, kv.Value))))
                    }
            }
        let shuffling =
            cloud {
                let combiner' (result : _ []) = local { return Array.concat result }
                let! totalWorkers = match source.DegreeOfParallelism with Some n -> local.Return n | None -> Cloud.GetWorkerCount()
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! kvs = source.WithEvaluators (collectorF cts.Token totalWorkers)
                                                 (fun kvs ->
                                                      local {
                                                         let dict = new Dictionary<int, PersistedCloudFlow<'Key * 'T>>()
                                                         for (k, kvs') in kvs do
                                                             let! pkvs = PersistedCloudFlow.New(kvs', storageLevel = StorageLevel.Disk)
                                                             dict.[k] <- pkvs;
                                                         return dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toArray
                                                      })
                                                 combiner'
                let merged =
                    kvs
                    |> Stream.ofArray
                    |> Stream.groupBy fst
                    |> Stream.map (fun (i,kva) -> i, kva |> Seq.map snd |> PersistedCloudFlow.Concat)
                    |> Stream.toArray
                return merged
            }
        let reducerF (cloudCt : ICloudCancellationToken) =
            local {
                let dict = new ConcurrentDictionary<'Key, 'T>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
                return
                    { new Collector<int * PersistedCloudFlow<'Key * 'T>, seq<'T>> with
                      member __.DegreeOfParallelism = source.DegreeOfParallelism
                      member __.Iterator() =
                          { Index = ref -1;
                            Func = (fun (_, pkvs) -> for (k, v) in pkvs do dict.TryAdd(k, v) |> ignore);
                            Cts = cts }
                      member __.Result = dict |> Seq.map (fun kv -> kv.Value)
                    }
            }
        let reducer (flow : CloudFlow<int * PersistedCloudFlow<'Key * 'T>>) : Cloud<PersistedCloudFlow<'T>> =
            cloud {
                let combiner' (result : PersistedCloudFlow<_> []) = local { return PersistedCloudFlow.Concat result }
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! pkvs = flow.WithEvaluators (reducerF cts.Token) (fun kvs -> PersistedCloudFlow.New(kvs, storageLevel = StorageLevel.Disk)) combiner'
                return pkvs
            }
        { new CloudFlow<'T> with
            member __.DegreeOfParallelism = source.DegreeOfParallelism
            member __.WithEvaluators<'S, 'R> (collectorF : CloudLocal<Collector<'T, 'S>>) (projection : 'S -> CloudLocal<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (Array.ToCloudFlow result)
                    return! (result' :> CloudFlow<_>).WithEvaluators collectorF projection combiner
                }
        }