namespace MBrace.Flow.Internals.Consumers

open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Flow
open MBrace.Flow.Internals

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
                (init : unit -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> = cloud {

        let collectorf = local {
            let! ct = Cloud.CancellationToken
            let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
            let results = new List<'State ref>()
            return 
                { new Collector<'T, 'State> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism
                    member self.Iterator() =
                        let accRef = ref <| init ()
                        results.Add(accRef)
                        {   Func = fun t -> accRef := folder !accRef t
                            Cts = cts }
                    member self.Result =
                        let mutable acc = init ()
                        for result in results do acc <- combiner acc !result
                        acc 
                }
        }

        let combine (values : 'State[]) =
            let mutable state = init ()
            for v in values do state <- combiner state v
            state

        return! flow.WithEvaluators collectorf local.Return (local.Return << combine)
    }

    /// <summary>
    ///     Generic fold by Key implementation
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="folder"></param>
    /// <param name="combiner"></param>
    /// <param name="state"></param>
    /// <param name="flow"></param>
    let foldBy (projection : 'T -> 'Key)
                    (folder : 'State -> 'T -> 'State)
                    (combiner : 'State -> 'State -> 'State)
                    (init : unit -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> =

        // Stage 1 : shuffle data to workers by computed key
        let shuffling () = cloud {
            // initial folding collector
            let collectorf = local {
                let! ct = Cloud.CancellationToken
                let dict = new ConcurrentDictionary<'Key, 'State ref>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return
                  { new Collector<'T,  seq<'Key * 'State>> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism
                    member self.Iterator() =
                        {   Func =
                                fun value ->
                                    let grouping = dict.GetOrAdd(projection value, fun _ -> ref <| init ())
                                    lock grouping (fun () -> grouping := folder !grouping value)

                            Cts = cts }
                    member self.Result = dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) }
            }

            // local shuffle of grouped results
            let shuffleLocal (groupings : seq<'Key * 'State>) = local {
                let! current = Cloud.CurrentWorker
                let! workers = Cloud.GetAvailableWorkers()
                let workers = 
                    match flow.DegreeOfParallelism with 
                    | None -> workers
                    | Some dp -> Array.init (min dp workers.Length) (fun i -> workers.[i % workers.Length])

                let getLevel w = if w = current then StorageLevel.Memory else StorageLevel.Disk

                return!
                    groupings
                    |> Seq.groupBy (fun (k,_) -> workers.[abs (hash k) % workers.Length])
                    |> Seq.map (fun (w,gps) -> local { let! ca = CloudValue.NewArray(gps, storageLevel = getLevel w) in return w, ca })
                    |> Local.Parallel
            }

            // top-level shuffled results combiner
            let combiner (gathered : (IWorkerRef * CloudArray<'Key * 'State>) [] []) = local {
                return gathered |> Seq.concat |> Seq.groupBy fst |> Seq.collect snd |> Seq.toArray
            }

            let! shuffleResults = flow.WithEvaluators collectorf shuffleLocal combiner
            return PersistedCloudFlow<_>(shuffleResults) :> CloudFlow<'Key * 'State>
        }

        // Stage 2 : Perform final reduction operation on shuffled data
        let reducerf (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) = local {
            let! ct = Cloud.CancellationToken
            let! collector = collectorf
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let threadCount = ref 0
            let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
            return 
                { new Collector<'Key * 'State, 'S> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism
                    member self.Iterator() =
                        let _ = System.Threading.Interlocked.Increment threadCount
                        {   Func =
                                fun (key,value) ->
                                    let grouping = dict.GetOrAdd(key, fun _ -> ref <| init ())
                                    lock grouping (fun () -> grouping := combiner !grouping value)
                            Cts = cts }

                    member self.Result =
                        dict
                        |> Collection.splitByPartitionCount !threadCount
                        |> Array.Parallel.iter (fun kvs -> let iter = collector.Iterator() in for kv in kvs do iter.Func(kv.Key, kv.Value.Value))

                        collector.Result
                }
        }

        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner = cloud {
                let! shuffledFlow = shuffling()
                return! shuffledFlow.WithEvaluators (reducerf collectorf) projection combiner
            }  
        }




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