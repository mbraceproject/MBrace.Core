namespace MBrace.Flow.Internals.Consumers

open System
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

        // Stage 1 : fold inputs and shuffle data to workers by computed key
        let foldAndShuffleByKey () = cloud {
            // initial folding collector
            let foldCollector = local {
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
                let! workers = Cloud.GetAvailableWorkers()
                let workers = 
                    match flow.DegreeOfParallelism with 
                    | None -> workers
                    | Some dp -> Array.init (min dp workers.Length) (fun i -> workers.[i % workers.Length])

                return!
                    groupings
                    |> Seq.groupBy (fun (k,_) -> workers.[abs (hash k) % workers.Length])
                    |> Seq.map (fun (w,gps) -> local { let! ca = CloudValue.NewArray(gps, storageLevel = StorageLevel.Disk) in return w, ca })
                    |> Local.Parallel
            }

            // top-level shuffled results combiner
            let combiner (gathered : (IWorkerRef * CloudArray<'Key * 'State>) [] []) = local {
                return gathered |> Seq.concat |> Seq.sortBy fst |> Seq.toArray
            }

            let! shuffleResults = flow.WithEvaluators foldCollector shuffleLocal combiner
            return PersistedCloudFlow<_>(shuffleResults) :> CloudFlow<'Key * 'State>
        }

        // Stage 2 : Perform final reduction operation on shuffled data
        let reduceFlow (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) 
                        (projection : 'S -> LocalCloud<'R>) (flowCombiner : 'R [] -> LocalCloud<'R>)
                        (shuffled : CloudFlow<'Key * 'State>) = cloud {

            let reduceCollectorF = local {
                let! ct = Cloud.CancellationToken
                let dict = new ConcurrentDictionary<'Key, 'State ref>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return 
                    { new Collector<'Key * 'State, ICollection<'Key * 'State>> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func =
                                    fun (key,value) ->
                                        let grouping = dict.GetOrAdd(key, fun _ -> ref <| init ())
                                        lock grouping (fun () -> grouping := combiner !grouping value)
                                Cts = cts }

                        member self.Result = dict |> Collection.map (fun kv -> kv.Key, kv.Value.Value)
                    }
            }

            let reduceProjection (inputs : ICollection<'Key * 'State>) = local {
                let! collector = collectorf
                let iterators = Array.init Environment.ProcessorCount (fun _ -> collector.Iterator())
                let! _ =
                    inputs
                    |> Collection.splitByPartitionCount iterators.Length
                    |> Seq.mapi (fun i kvs -> local { let iter = iterators.[i] in return for kv in kvs do iter.Func kv })
                    |> Local.Parallel

                return! projection collector.Result
            }

            return! shuffled.WithEvaluators reduceCollectorF reduceProjection flowCombiner
        }

        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> collectorf (projection : 'S -> LocalCloud<'R>) combiner = cloud {
                let! shuffledFlow = foldAndShuffleByKey()
                return! reduceFlow collectorf projection combiner shuffledFlow
            }  
        }

    /// <summary>
    ///     Generic fold by key implementation
    /// </summary>
    /// <param name="projection"></param>
    /// <param name="folder"></param>
    /// <param name="combiner"></param>
    /// <param name="state"></param>
    /// <param name="flow"></param>
    let foldBy2 (projection : 'T -> 'Key)
                    (projection' : 'T0 -> 'Key)
                    (folder : 'State -> 'T -> 'State)
                    (folder' : 'State -> 'T0 -> 'State)
                    (combiner : 'State -> 'State -> 'State)
                    (init : unit -> 'State) (flow : CloudFlow<'T>) (flow' : CloudFlow<'T0>) : CloudFlow<'Key * 'State> =

        // Stage 1 : fold inputs and shuffle data to workers by computed key
        let foldAndShuffleByKey () = cloud {
            let mkFoldCollector (projection : 'S -> 'Key) (folder : 'State -> 'S -> 'State) = local {
                let! ct = Cloud.CancellationToken
                let dict = new ConcurrentDictionary<'Key, 'State ref>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return  
                    { new Collector<'S,  seq<'Key * 'State>> with
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
                let! workers = Cloud.GetAvailableWorkers()
                let workers = 
                    match flow.DegreeOfParallelism with 
                    | None -> workers
                    | Some dp -> Array.init (min dp workers.Length) (fun i -> workers.[i % workers.Length])

                return!
                    groupings
                    |> Seq.groupBy (fun (k,_) -> workers.[abs (hash k) % workers.Length])
                    |> Seq.map (fun (w,gps) -> local { let! ca = CloudValue.NewArray(gps, storageLevel = StorageLevel.Disk) in return w, ca })
                    |> Local.Parallel
            }

            // top-level shuffled results combiner
            let combiner (gathered : (IWorkerRef * CloudArray<'Key * 'State>) [] []) = local {
                return gathered |> Seq.concat |> Seq.sortBy fst |> Seq.toArray
            }

            let! shuffleResults1, shuffleResults2 = 
                (flow.WithEvaluators (mkFoldCollector projection folder) shuffleLocal combiner)
                    <||>
                (flow'.WithEvaluators (mkFoldCollector projection' folder') shuffleLocal combiner)

            let! combinedShuffle = combiner [|shuffleResults1 ; shuffleResults2|]
            return new PersistedCloudFlow<_>(combinedShuffle) :> CloudFlow<'Key * 'State>
        }

        // Stage 2 : Perform final reduction operation on shuffled data
        let reduceFlow (collectorf : LocalCloud<Collector<'Key * 'State, 'S>>) 
                        (projection : 'S -> LocalCloud<'R>) (flowCombiner : 'R [] -> LocalCloud<'R>)
                        (shuffled : CloudFlow<'Key * 'State>) = cloud {

            let reduceCollectorF = local {
                let! ct = Cloud.CancellationToken
                let dict = new ConcurrentDictionary<'Key, 'State ref>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return 
                    { new Collector<'Key * 'State, ICollection<'Key * 'State>> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func =
                                    fun (key,value) ->
                                        let grouping = dict.GetOrAdd(key, fun _ -> ref <| init ())
                                        lock grouping (fun () -> grouping := combiner !grouping value)
                                Cts = cts }

                        member self.Result = dict |> Collection.map (fun kv -> kv.Key, kv.Value.Value)
                    }
            }

            let reduceProjection (inputs : ICollection<'Key * 'State>) = local {
                let! collector = collectorf
                let iterators = Array.init Environment.ProcessorCount (fun _ -> collector.Iterator())
                let! _ =
                    inputs
                    |> Collection.splitByPartitionCount iterators.Length
                    |> Seq.mapi (fun i kvs -> local { let iter = iterators.[i] in return for kv in kvs do iter.Func kv })
                    |> Local.Parallel

                return! projection collector.Result
            }

            return! shuffled.WithEvaluators reduceCollectorF reduceProjection flowCombiner
        }

        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> collectorf (projection : 'S -> LocalCloud<'R>) combiner = cloud {
                let! shuffledFlow = foldAndShuffleByKey()
                return! reduceFlow collectorf projection combiner shuffledFlow
            }  
        }