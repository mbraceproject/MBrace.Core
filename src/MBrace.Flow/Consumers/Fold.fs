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
    let fold (folder : 'State -> 'T -> 'State) 
                (combiner : 'State -> 'State -> 'State)
                (state : unit -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> =

        let collectorf (ct : ICloudCancellationToken) = local {
            let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
            let results = new List<'State ref>()
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    let accRef = ref <| state ()
                    results.Add(accRef)
                    {   Func = fun t -> accRef := folder !accRef t
                        Cts = cts }
                member self.Result =
                    let mutable acc = state ()
                    for result in results do
                            acc <- combiner acc !result
                    acc }
        }
        cloud {
            let! ct = Cloud.CancellationToken
            return!
                flow.WithEvaluators
                   (collectorf ct)
                   (fun x -> local { return x })
                   (fun values -> local {
                       let combined =
                            let mutable state = state ()
                            for v in values do state <- combiner state v
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
    let foldByGen (projection : 'T -> 'Key)
                    (folder : 'State -> 'T -> 'State)
                    (combiner : 'State -> 'State -> 'State)
                    (state : unit -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> =

        let collectorf (ct : ICloudCancellationToken) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(ct.LocalToken)
            return
              { new Collector<'T,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
                            fun value ->
                                let grouping = dict.GetOrAdd(projection value, fun _ -> ref <| state ())
                                lock grouping (fun () -> grouping := folder !grouping value)

                        Cts = cts }
                member self.Result = dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) }
        }

        // Phase 1 : shuffle data by key
        let shuffling () = cloud {
            let! workers = Cloud.GetAvailableWorkers()
            let workers = 
                match flow.DegreeOfParallelism with 
                | None -> workers
                | Some dp -> Array.init dp (fun i -> workers.[i % workers.Length])

//            let combiner' (results : _ []) = local { return PersistedCloudFlow.Concat results }
            let shuffle (groupings : seq<'Key * 'State>) = local {
                let! pf =
                    groupings
                    |> Seq.groupBy (fun (k,_) -> workers.[abs (hash k) % workers.Length])
                    |> Seq.map (fun (w,gps) -> PersistedCloudFlow.New(gps, storageLevel = StorageLevel.Disk, targetWorker = w))
                    |> Local.Parallel

                return! combiner' pf
            }

            let! ct = Cloud.CancellationToken
            return! flow.WithEvaluators (collectorf ct) shuffle combiner'
        }

        let reducerf (ct : ICloudCancellationToken) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(ct.LocalToken)
            return { new Collector<IWorkerRef * PersistedCloudFlow<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    {   Func =
                            (fun (_, keyValues) ->
                                for (key, value) in keyValues do
                                    let mutable grouping = Unchecked.defaultof<_>
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner !acc value)
                                    else
                                        grouping <- ref <| state ()
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := combiner !acc value)
                                ());
                        Cts = cts }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }

//        // Phase 2
//        let reducer (flow : CloudFlow<int * PersistedCloudFlow<'Key * 'State>>) : Cloud<PersistedCloudFlow<'Key * 'State>> =
//            cloud {
//                let combiner' (result : PersistedCloudFlow<_> []) = local { return PersistedCloudFlow.Concat result }
//                use! cts = Cloud.CreateLinkedCancellationTokenSource()
//                let! keyValueArray = flow.WithEvaluators (reducerf cts.Token) (fun keyValues -> PersistedCloudFlow.New(keyValues, storageLevel = StorageLevel.Disk)) combiner'
//                return keyValueArray
//            }

        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner =
                cloud {
                    let! result = shuffling()
                    return! (result :> CloudFlow<_>).WithEvaluators collectorf projection combiner
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
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (Array.ToCloudFlow result)
                    return! (result' :> CloudFlow<_>).WithEvaluators collectorf projection combiner
                }  }