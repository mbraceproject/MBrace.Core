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

        let collectorf (cloudCt : ICloudCancellationToken) = local {
            let results = new List<'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    let accRef = ref <| state ctx
                    results.Add(accRef)
                    {   Func = (fun value -> accRef := folder ctx !accRef value);
                        Cts = cts }
                member self.Result =
                    let mutable acc = state ctx
                    for result in results do
                            acc <- combiner ctx acc !result
                    acc }
        }
        cloud {
            use! cts = Cloud.CreateLinkedCancellationTokenSource()
            return!
                flow.WithEvaluators
                   (collectorf cts.Token)
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

        let collectorf (cloudCt : ICloudCancellationToken) (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
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
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (collectorf cts.Token totalWorkers)
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
        let reducerf (cloudCt : ICloudCancellationToken) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
            return { new Collector<int * PersistedCloudFlow<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
                            (fun (_, keyValues) ->
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
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (reducerf cts.Token) (fun keyValues -> PersistedCloudFlow.New(keyValues, storageLevel = StorageLevel.Disk)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : CloudLocal<Collector<'Key * 'State, 'S>>) (projection : 'S -> CloudLocal<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (Array.ToCloudFlow result)
                    return! (result' :> CloudFlow<_>).WithEvaluators collectorf projection combiner
                }  }




    /// <summary>
    ///     Generic fold by Key implementation
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="folder"></param>
    /// <param name="combiner"></param>
    /// <param name="state"></param>
    /// <param name="flow"></param>
    let foldByGen2  (projection : ExecutionContext -> 'T -> 'Key)
                    (projection' : ExecutionContext -> 'R -> 'Key)
                    (folder : ExecutionContext -> 'State -> 'T -> 'State)
                    (folder' : ExecutionContext -> 'State -> 'R -> 'State)
                    (combiner : ExecutionContext -> 'State -> 'State -> 'State)
                    (state : ExecutionContext -> 'State) (flow : CloudFlow<'T>) (flow' : CloudFlow<'R>) : CloudFlow<'Key * 'State> =

        let collectorf (projection : ExecutionContext -> 'S -> 'Key) (folder : ExecutionContext -> 'State -> 'S -> 'State) (cloudCt : ICloudCancellationToken) (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
            return
              { new Collector<'S,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
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
        let runFlow  (projection : ExecutionContext -> 'S -> 'Key) (folder : ExecutionContext -> 'State -> 'S -> 'State) (flow : CloudFlow<'S>) =
            cloud {
                let combiner' (result : _ []) = local { return Array.concat result }
                let! totalWorkers = match flow.DegreeOfParallelism with Some n -> local { return n } | None -> Cloud.GetWorkerCount()
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (collectorf projection folder cts.Token totalWorkers)
                                                    (fun keyValues -> local {
                                                        let dict = new Dictionary<int, PersistedCloudFlow<'Key * 'State>>()
                                                        for (key, value) in keyValues do
                                                            // partition in entities of 1GB
                                                            let! values = PersistedCloudFlow.New(value, storageLevel = StorageLevel.Disk)
                                                            dict.[key] <- values
                                                        let values = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value))
                                                        return Seq.toArray values }) combiner'
                return keyValueArray 
            }
        // Phase 1
        let shuffling =
            cloud {
                let! keyValueArray = runFlow projection folder flow 
                let! keyValueArray' = runFlow projection' folder' flow'
                let merged =
                    keyValueArray 
                    |> Array.append keyValueArray'
                    |> Stream.ofArray
                    |> Stream.groupBy fst
                    |> Stream.map (fun (i,kva) -> i, kva |> Seq.map snd |> PersistedCloudFlow.Concat)
                    |> Stream.toArray
                return merged
            }
        let reducerf (cloudCt : ICloudCancellationToken) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCt.LocalToken)
            return { new Collector<int * PersistedCloudFlow<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
                            (fun (_, keyValues) ->
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
                use! cts = Cloud.CreateLinkedCancellationTokenSource()
                let! keyValueArray = flow.WithEvaluators (reducerf cts.Token) (fun keyValues -> PersistedCloudFlow.New(keyValues, storageLevel = StorageLevel.Disk)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : CloudLocal<Collector<'Key * 'State, 'S>>) (projection : 'S -> CloudLocal<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (Array.ToCloudFlow result)
                    return! (result' :> CloudFlow<_>).WithEvaluators collectorf projection combiner
                }  }