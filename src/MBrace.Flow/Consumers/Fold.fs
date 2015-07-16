namespace MBrace.Flow.Internals.Consumers

open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Fold =

    /// <summary>
    ///     Generic folding consumer
    /// </summary>
    /// <param name="folder"></param>
    /// <param name="combiner"></param>
    /// <param name="state"></param>
    /// <param name="flow"></param>
    let foldGen (folder : ExecutionContext -> 'State -> 'T -> 'State) 
                (combiner : ExecutionContext -> 'State -> 'State -> 'State)
                (state : ExecutionContext -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> =

        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {
            let results = new List<'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    let accRef = ref <| state ctx
                    results.Add(accRef)
                    {   Index = ref -1;
                        Func = (fun value -> accRef := folder ctx !accRef value);
                        Cts = cts }
                member self.Result =
                    let mutable acc = state ctx
                    for result in results do
                            acc <- combiner ctx acc !result
                    acc }
        }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return!
                flow.WithEvaluators
                   (collectorf cts)
                   (fun x -> local { return x })
                   (fun values -> local {
                       let! ctx = Cloud.GetExecutionContext()
                       let combined =
                            let mutable state = state ctx
                            for v in values do state <- combiner ctx state v
                            state
                       return combined })
        }

    /// <summary>
    ///     Generic fold by Key implementation
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="folder"></param>
    /// <param name="combiner"></param>
    /// <param name="state"></param>
    /// <param name="flow"></param>
    let foldByGen (projection : ExecutionContext -> 'T -> 'Key)
                    (folder : ExecutionContext -> 'State -> 'T -> 'State)
                    (combiner : ExecutionContext -> 'State -> 'State -> 'State)
                    (state : ExecutionContext -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> =

        let collectorf (cloudCts : ICloudCancellationTokenSource) (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Index = ref -1;
                        Func =
                            (fun value ->
                                    let mutable grouping = Unchecked.defaultof<_>
                                    let key = projection ctx value
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder ctx !acc value)
                                    else
                                        grouping <- ref <| state ctx
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder ctx !acc value)
                                    ());
                        Cts = cts }
                member self.Result =
                    let partitions = dict
                                     |> Seq.groupBy (fun keyValue -> abs (keyValue.Key.GetHashCode()) % totalWorkers)
                                     |> Seq.map (fun (key, keyValues) -> (key, keyValues |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value))))
                    partitions }
        }
        // Phase 1
        let shuffling =
            cloud {
                let combiner' (result : _ []) = local { return Array.concat result }
                let! totalWorkers = match flow.DegreeOfParallelism with Some n -> local { return n } | None -> Cloud.GetWorkerCount()
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (collectorf cts totalWorkers)
                                                  (fun keyValues -> local {
                                                        let dict = new Dictionary<int, PersistedCloudFlow<'Key * 'State>>()
                                                        for (key, value) in keyValues do
                                                            // partition in entities of 1GB
                                                            let! values = PersistedCloudFlow.New(value, storageLevel = StorageLevel.Disk)
                                                            dict.[key] <- values
                                                        let values = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value))
                                                        return Seq.toArray values }) combiner'

                let merged =
                    keyValueArray
                    |> Stream.ofArray
                    |> Stream.groupBy fst
                    |> Stream.map (fun (i,kva) -> i, kva |> Seq.map snd |> PersistedCloudFlow.Concat)
                    |> Stream.toArray
                return merged
            }
        let reducerf (cloudCts : ICloudCancellationTokenSource) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return { new Collector<int * PersistedCloudFlow<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Index = ref -1;
                        Func =
                            (fun (_, keyValues) ->
                                let keyValues = keyValues.ToEnumerable()

                                for (key, value) in keyValues do
                                    let mutable grouping = Unchecked.defaultof<_>
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner ctx !acc value)
                                    else
                                        grouping <- ref <| state ctx
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner ctx !acc value)
                                ());
                        Cts = cts }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }
        // Phase 2
        let reducer (flow : CloudFlow<int * PersistedCloudFlow<'Key * 'State>>) : Cloud<PersistedCloudFlow<'Key * 'State>> =
            cloud {
                let combiner' (result : PersistedCloudFlow<_> []) = local { return PersistedCloudFlow.Concat result }
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (reducerf cts) (fun keyValues -> PersistedCloudFlow.New(keyValues, storageLevel = StorageLevel.Disk)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : Local<Collector<'Key * 'State, 'S>>) (projection : 'S -> Local<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (Array.ToCloudFlow result)
                    return! (result' :> CloudFlow<_>).WithEvaluators collectorf projection combiner
                }  }