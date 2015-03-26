namespace MBrace.Flow

#nowarn "0443"
#nowarn "0444"

open System
open System.Threading
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq
open MBrace
open MBrace.Continuation
open MBrace.Workflows
open Nessos.Streams
open Nessos.Streams.Internals


/// Collects elements into a mutable result container.
type Collector<'T, 'R> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option
    /// Gets an iterator over the elements.
    abstract Iterator : unit -> ParIterator<'T>
    /// The result of the collector.
    abstract Result : 'R

/// Represents a distributed Stream of values.
type CloudFlow<'T> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option
    /// Applies the given collector to the CloudFlow.
    abstract Apply<'S, 'R> : Local<Collector<'T, 'S>> -> ('S -> Local<'R>) -> ('R []  -> Local<'R>) -> Cloud<'R>

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudFlows.
module CloudFlow =

    //#region Helpers

    /// Maximum combined stream length used in ofCloudFiles.
    let private maxCloudFileCombinedLength = 1024L * 1024L * 1024L
    
    /// Maximum CloudVector partition size used in CloudVector.New.
    [<Literal>]
    let private maxCloudVectorPartitionSize = 1073741824L // 1GB


    /// gets all partition indices found in cloud vector
    let inline private getPartitionIndices (v : CloudVector<'T>) = [| 0 .. v.PartitionCount - 1 |]


    /// Converts MBrace.Flow.Collector to Nessos.Streams.Collector
    let inline private toParStreamCollector (collector : Collector<'T, 'S>) =
        { new Nessos.Streams.Collector<'T, 'S> with
            member self.DegreeOfParallelism = match collector.DegreeOfParallelism with Some n -> n | None -> Environment.ProcessorCount
            member self.Iterator() = collector.Iterator()
            member self.Result = collector.Result  }

    //#endregion

    //#region Driver functions

    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    let ofArray (source : 'T []) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! collector = collectorf 
                    let! workerCount = 
                        match collector.DegreeOfParallelism with
                        | Some n -> local { return n }
                        | _ -> Cloud.GetWorkerCount()
                    let! workers = Cloud.GetAvailableWorkers()

                    let createTask array (collector : Local<Collector<'T, 'S>>) = 
                        local {
                            let! collector = collector
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply (toParStreamCollector collector)
                            return! projection collectorResult
                        }
                    if not (source.Length = 0) then 
                        let partitions = Partitions.ofLongRange workerCount (int64 source.Length)
                        let! results = 
                            partitions 
                            |> Array.mapi (fun i (s, e) -> 
                                                let cloudBlock = createTask [| for i in s..(e - 1L) do yield source.[int i] |] collectorf
                                                (cloudBlock, workers.[i % workers.Length])) 
                            |> Cloud.Parallel
                        return! combiner results 
                    else
                        return! projection collector.Result
                } }

    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFiles (reader : System.IO.Stream -> Async<'T>) (sources : seq<CloudFile>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud { 
                    if Seq.isEmpty sources then 
                        let! collector = collectorf
                        return! projection collector.Result
                    else
                        let! collector = collectorf
                        let! workerCount = 
                            match collector.DegreeOfParallelism with
                            | Some n -> local { return n }
                            | _ -> Cloud.GetWorkerCount()
                        let! workers = Cloud.GetAvailableWorkers() 

                        let createTask (files : CloudFile []) (collectorf : Local<Collector<'T, 'S>>) : Local<'R> = 
                            local {
                                let rec partitionByLength (files : CloudFile []) index (currLength : int64) (currAcc : CloudFile list) (acc : CloudFile list list)=
                                    local {
                                        if index >= files.Length then return (currAcc :: acc) |> List.filter (not << List.isEmpty)
                                        else
                                            let! length = CloudFile.GetSize(files.[index])
                                            if length >= maxCloudFileCombinedLength then
                                                return! partitionByLength files (index + 1) length [files.[index]] (currAcc :: acc)
                                            elif length + currLength >= maxCloudFileCombinedLength then
                                                return! partitionByLength files index 0L [] (currAcc :: acc)
                                            else
                                                return! partitionByLength files (index + 1) (currLength + length) (files.[index] :: currAcc) acc
                                    }
                                let! partitions = partitionByLength files 0 0L [] []

                                let result = new ResizeArray<'R>(partitions.Length)
                                let! ctx = Cloud.GetExecutionContext()
                                for fs in partitions do
                                    let! collector = collectorf
                                    let parStream = 
                                        fs
                                        |> ParStream.ofSeq 
                                        |> ParStream.map (fun file -> CloudFile.Read(file, reader, leaveOpen = true))
                                        |> ParStream.map (fun wf -> Cloud.RunSynchronously(wf, ctx.Resources, ctx.CancellationToken))
                                    let collectorResult = parStream.Apply (toParStreamCollector collector)
                                    let! partial = projection collectorResult
                                    result.Add(partial)
                                if result.Count = 0 then
                                    let! collector = collectorf
                                    return! projection collector.Result
                                else
                                    return! combiner (result.ToArray())
                            }

                        let partitions = sources |> Seq.toArray |> Partitions.ofArray workerCount
                        let! results = 
                            partitions 
                            |> Array.mapi (fun i cfiles -> (createTask cfiles collectorf, workers.[i % workers.Length])) 
                            |> Cloud.Parallel
                        return! combiner results
                } }

    /// <summary>
    /// Constructs a CloudFlow from a CloudVector.
    /// </summary>
    /// <param name="source">The input CloudVector.</param>
    let ofCloudVector (source : CloudVector<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let useCache = source.IsCachingSupported
                    let partitions = getPartitionIndices source


                    let computePartitions (partitions : int []) = local {

                        let computePartition (pIndex : int) = local {
                            let partition = source.GetPartition pIndex
                            let! collector = collectorf
                            if useCache then do! partition.PopulateCache() |> Local.Ignore
                            let! array = partition.ToArray()
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply (toParStreamCollector collector)
                            return! projection collectorResult
                        }

                        /// use sequential computation; should probably allow some degree of parallelization
                        let! results = Sequential.map computePartition partitions

                        // do not allow cache state updating in stream execution under local runtimes
                        if useCache then
                            let! worker = Cloud.CurrentWorker
                            do! source.UpdateCacheState(worker, partitions)

                        return! combiner results
                    }

                    if Array.isEmpty partitions then 
                        let! collector = collectorf
                        return! projection collector.Result 
                    else
                        let! collector = collectorf
                        let! workerCount = local {
                            match collector.DegreeOfParallelism with
                            | Some n -> return n
                            | None -> return! Cloud.GetWorkerCount()
                        }
                        let! workers = Cloud.GetAvailableWorkers()

                        // TODO : need a scheduling algorithm to assign partition indices
                        //        to workers according to current cache state.
                        //        for now blindly assign partitions among workers.

                        let! results =
                            partitions
                            |> Partitions.ofArray workerCount
                            |> Seq.filter (not << Array.isEmpty)
                            |> Seq.mapi (fun i partitions -> (computePartitions partitions, workers.[i % workers.Length]))
                            |> Cloud.Parallel

                        return! combiner results
                }
        }

    //#endregion

    //#region Intermediate functions

    let inline private run ctx a = Cloud.RunSynchronously(a, ctx.Resources,ctx.CancellationToken)

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline private mapGen  (f : ExecutionContext -> 'T -> 'R) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return 
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = (fun value -> iter (f ctx value));
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline map  (f : 'T -> 'R) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun ctx x -> f x) stream

    /// <summary>Transforms each element of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline mapLocal (f : 'T -> Local<'R>) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun ctx x -> f x |> run ctx) stream

    let inline private collectGen (f : ExecutionContext -> 'T -> seq<'R>) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return 
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = 
                                    (fun value -> 
                                        let (Stream streamf) = Stream.ofSeq (f ctx value)
                                        let cts = CancellationTokenSource.CreateLinkedTokenSource(iterator.Cts.Token)
                                        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ()); Cont = iter; Cts = cts } in bulk ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collect (f : 'T -> seq<'R>) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun ctx x -> f x) stream 

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collectLocal (f : 'T -> Local<seq<'R>>) (stream : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun ctx x -> f x |> run ctx) stream 

    let inline private filterGen (predicate : ExecutionContext -> 'T -> bool) (stream : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = (fun value -> if predicate ctx value then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                stream.Apply collectorf' projection combiner }

    /// <summary>Filters the elements of the input CloudFlow.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filter (predicate : 'T -> bool) (stream : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun ctx x -> predicate x) stream 

    /// <summary>Filters the elements of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filterLocal (predicate : 'T -> Local<bool>) (stream : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun ctx x -> predicate x |> run ctx) stream 

    /// <summary>Returns a cloud stream with a new degree of parallelism.</summary>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The result cloud stream.</returns>
    let inline withDegreeOfParallelism (degreeOfParallelism : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else
            { new CloudFlow<'T> with
                    member self.DegreeOfParallelism = Some degreeOfParallelism
                    member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                        stream.Apply collectorf projection combiner }

    // terminal functions

    let inline private foldGen (folder : ExecutionContext -> 'State -> 'T -> 'State) (combiner : ExecutionContext -> 'State -> 'State -> 'State) 
                               (state : ExecutionContext -> 'State) (stream : CloudFlow<'T>) : Cloud<'State> =
        let collectorf = local {  
            let results = new List<'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                member self.Iterator() = 
                    let accRef = ref <| state ctx 
                    results.Add(accRef)
                    {   Index = ref -1;
                        Func = (fun value -> accRef := folder ctx !accRef value);
                        Cts = new CancellationTokenSource() }
                member self.Result = 
                    let mutable acc = state ctx
                    for result in results do
                            acc <- combiner ctx acc !result
                    acc }
        }
        stream.Apply 
           collectorf 
           (fun x -> local { return x }) 
           (fun values -> local { 
               let! ctx = Cloud.GetExecutionContext()
               return Array.reduce (combiner ctx) values })



    /// <summary>Applies a locally executing cloud function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline foldLocal (folder : 'State -> 'T -> Local<'State>) (combiner : 'State -> 'State -> Local<'State>) 
                         (state : unit -> Local<'State>) (stream : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun ctx x y -> run ctx (folder x y)) (fun ctx x y -> run ctx (combiner x y)) (fun ctx -> run ctx (state ())) stream

    /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun ctx x y -> folder x y) (fun ctx x y -> combiner x y) (fun ctx -> state ()) stream

    let inline private foldByGen (projection : ExecutionContext -> 'T -> 'Key) 
                                 (folder : ExecutionContext -> 'State -> 'T -> 'State) 
                                 (combiner : ExecutionContext -> 'State -> 'State -> 'State) 
                                 (state : ExecutionContext -> 'State) (stream : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        let collectorf (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
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
                        Cts = new CancellationTokenSource() }
                member self.Result = 
                    let partitions = dict 
                                     |> Seq.groupBy (fun keyValue -> Math.Abs(keyValue.Key.GetHashCode()) % totalWorkers)
                                     |> Seq.map (fun (key, keyValues) -> (key, keyValues |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value))))
                    partitions }
        }
        // Phase 1
        let shuffling = 
            cloud {
                let combiner' (result : _ []) = local { return Array.reduce Array.append result }
                let! totalWorkers = match stream.DegreeOfParallelism with Some n -> local { return n } | None -> Cloud.GetWorkerCount()
                let! keyValueArray = stream.Apply (collectorf totalWorkers) 
                                                  (fun keyValues -> local {
                                                        let dict = new Dictionary<int, CloudVector<'Key * 'State>>() 
                                                        for (key, value) in keyValues do
                                                            let! values = CloudVector.New(value, maxCloudVectorPartitionSize, enableCaching = false)
                                                            dict.[key] <- values
                                                        let values = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value)) 
                                                        return Seq.toArray values }) combiner'
                
                let merged =
                    keyValueArray
                    |> Seq.groupBy fst
                    |> Seq.map (fun (i,kva) -> i, kva |> Seq.map snd |> CloudVector.Merge)
                    |> Seq.toArray
                return merged
            }
        let reducerf = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            return { new Collector<int * CloudVector<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                member self.Iterator() = 
                    {   Index = ref -1; 
                        Func =
                            (fun (_, keyValues) ->
                                let keyValues = Cloud.RunSynchronously(keyValues.ToEnumerable(), ctx.Resources, ctx.CancellationToken)
                                   
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
                        Cts = new CancellationTokenSource() }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }
        // Phase 2
        let reducer (stream : CloudFlow<int * CloudVector<'Key * 'State>>) : Cloud<CloudVector<'Key * 'State>> = 
            cloud {
                let combiner' (result : CloudVector<_> []) = local { return CloudVector.Merge result }

                let! keyValueArray = stream.Apply reducerf (fun keyValues -> CloudVector.New(keyValues, maxCloudVectorPartitionSize)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'Key * 'State, 'S>>) (projection : 'S -> Local<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (ofArray result)
                    return! (ofCloudVector result').Apply collectorf projection combiner
                }  }


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator. The folder, combiner and state are locally executing cloud functions.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline foldByLocal (projection : 'T -> Local<'Key>) 
                           (folder : 'State -> 'T -> Local<'State>) 
                           (combiner : 'State -> 'State -> Local<'State>) 
                           (state : unit -> Local<'State>) (stream : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun ctx x -> projection x |> run ctx) (fun ctx x y -> folder x y |> run ctx) (fun ctx s1 s2 -> combiner s1 s2 |> run ctx) (fun ctx -> state () |> run ctx) stream 

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (stream : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun ctx x -> projection x) (fun ctx x y -> folder x y) (fun ctx s1 s2 -> combiner s1 s2) (fun ctx -> state ()) stream 

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun _ctx x -> projection x) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun _ctx -> 0L) stream

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    let inline countByLocal (projection : 'T -> Local<'Key>) (stream : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun ctx x -> projection x |> run ctx) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun ctx -> 0L) stream

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iter (action: 'T -> unit) (stream : CloudFlow< 'T >) : Cloud< unit > =
        fold (fun () x -> action x) (fun () () -> ()) (fun () -> ()) stream

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iterLocal (action: 'T -> Local<unit>) (stream : CloudFlow< 'T >) : Cloud< unit > =
        foldLocal (fun () x -> action x) (fun () () -> local { return () }) (fun () -> local { return () }) stream

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : CloudFlow< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumBy projection (stream : CloudFlow< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (fun s x -> s + projection x) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    /// <summary>Applies a key-generating locally executing cloud function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumByLocal (projection : ^T -> Local< ^Key >) (stream : CloudFlow< ^T >) : Cloud< ^Key > 
            when ^Key : (static member ( + ) : ^Key * ^Key -> ^Key) 
            and  ^Key : (static member Zero : ^Key) = 
        foldGen (fun ctx s x -> s + run ctx (projection x)) (fun _ctx x y -> x + y) (fun _ctx -> LanguagePrimitives.GenericZero) stream

    /// <summary>Returns the total number of elements of the CloudFlow.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (stream : CloudFlow<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) stream

    /// <summary>Creates an array from the given CloudFlow.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (stream : CloudFlow<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            return arrayCollector.ToArray()
        }

    // Taken from FSharp.Core
    //
    // The CLI implementation of mscorlib optimizes array sorting
    // when the comparer is either null or precisely
    // reference-equals to System.Collections.Generic.Comparer<'T>.Default.
    // This is an indication that a "fast" array sorting helper can be used.
    //
    // This type is only public because of the excessive inlining used in this file
    type _PrivateFastGenericComparerTable<'T when 'T : comparison>() = 

        static let fCanBeNull : System.Collections.Generic.IComparer<'T>  = 
            match typeof<'T> with 
            | ty when ty.Equals(typeof<byte>)       -> null    
            | ty when ty.Equals(typeof<char>)       -> null    
            | ty when ty.Equals(typeof<sbyte>)      -> null     
            | ty when ty.Equals(typeof<int16>)      -> null    
            | ty when ty.Equals(typeof<int32>)      -> null    
            | ty when ty.Equals(typeof<int64>)      -> null    
            | ty when ty.Equals(typeof<uint16>)     -> null    
            | ty when ty.Equals(typeof<uint32>)     -> null    
            | ty when ty.Equals(typeof<uint64>)     -> null    
            | ty when ty.Equals(typeof<float>)      -> null    
            | ty when ty.Equals(typeof<float32>)    -> null    
            | ty when ty.Equals(typeof<decimal>)    -> null    
            | _ -> LanguagePrimitives.FastGenericComparer<'T>

        static member ValueCanBeNullIfDefaultSemantics : System.Collections.Generic.IComparer<'T> = fCanBeNull

    /// <summary>Creates a CloudVector from the given CloudFlow.</summary>
    /// <param name="stream">The input CloudFlow.</param>
    /// <returns>The result CloudVector.</returns>    
    let inline toCloudVector (stream : CloudFlow<'T>) : Cloud<CloudVector<'T>> =
        cloud {
            let collectorf = local { 
                let results = new List<List<'T>>()
                return 
                  { new Collector<'T, 'T []> with
                    member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                    member self.Iterator() = 
                        let list = new List<'T>()
                        results.Add(list)
                        {   Index = ref -1; 
                            Func = (fun value -> list.Add(value));
                            Cts = new CancellationTokenSource() }
                    member self.Result = 
                        let count = results |> Seq.sumBy (fun list -> list.Count)
                        let values = Array.zeroCreate<'T> count
                        let mutable counter = -1
                        for list in results do
                            for i = 0 to list.Count - 1 do
                                let value = list.[i]
                                counter <- counter + 1
                                values.[counter] <- value
                        values }
            }

            let! vc =
                stream.Apply collectorf 
                    (fun array -> local { return! CloudVector.New(array, maxCloudVectorPartitionSize, enableCaching = false) }) 
                    (fun result -> local { return CloudVector.Merge result })
            return vc
        }

    let inline private sortByGen comparer (projection : ExecutionContext -> 'T -> 'Key) (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        let collectorf = local {  
            let results = new List<List<'T>>()
            let! ctx = Cloud.GetExecutionContext()
            return 
              { new Collector<'T, List<'Key[] * 'T []>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    {   Index = ref -1; 
                        Func = (fun value -> list.Add(value));
                        Cts = new CancellationTokenSource() }
                member self.Result = 
                    let count = results |> Seq.sumBy (fun list -> list.Count)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for list in results do
                        for i = 0 to list.Count - 1 do
                            let value = list.[i]
                            counter <- counter + 1
                            keys.[counter] <- projection ctx value
                            values.[counter] <- value
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(), 
                                     values.Take(takeCount).ToArray())) }
        }
        let sortByComp = 
            cloud {
                let! results = stream.Apply collectorf (fun x -> local { return x }) (fun result -> local { return Array.reduce (fun left right -> left.AddRange(right); left) result })
                let result = 
                    let count = results |> Seq.sumBy (fun (keys, _) -> keys.Length)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for (keys', values') in results do
                        for i = 0 to keys'.Length - 1 do
                            counter <- counter + 1
                            keys.[counter] <- keys'.[i]
                            values.[counter] <- values'.[i]
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    values.Take(takeCount).ToArray()
                return result
            }
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (ofArray result).Apply collectorf projection combiner
                }  
        }

    let inline private descComparer (comparer: IComparer<'T>) = { new IComparer<'T> with member __.Compare(x,y) = -comparer.Compare(x,y) }

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun _ctx x -> projection x) takeCount stream 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered using the given comparer for the keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsing (projection : 'T -> 'Key) comparer (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun _ctx x -> projection x) takeCount stream 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered descending by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescending (projection : 'T -> 'Key) (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun _ctx x -> projection x) takeCount stream 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByLocal (projection : 'T -> Local<'Key>) (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount stream 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsingLocal (projection : 'T -> Local<'Key>) comparer (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount stream 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by descending keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="stream">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescendingLocal (projection : 'T -> Local<'Key>) (takeCount : int) (stream : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount stream 

    let inline private tryFindGen (predicate : ExecutionContext -> 'T -> bool) (stream : CloudFlow<'T>) : Cloud<'T option> =
        let collectorf = 
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts =  new CancellationTokenSource()
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> if predicate ctx value then resultRef := Some value; cts.Cancel() else ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            return! stream.Apply collectorf (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun _ctx x -> predicate x) stream 

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFindLocal (predicate : 'T -> Local<bool>) (stream : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun ctx x -> predicate x |> run ctx) stream 

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud stream.</exception>
    let inline find (predicate : 'T -> bool) (stream : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFind predicate stream 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud stream.</exception>
    let inline findLocal (predicate : 'T -> Local<bool>) (stream : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFindLocal predicate stream 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    let inline private tryPickGen (chooser : ExecutionContext -> 'T -> 'R option) (stream : CloudFlow<'T>) : Cloud<'R option> = 
        
        let collectorf = 
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'R option>
                let cts = new CancellationTokenSource()
                return 
                    { new Collector<'T, 'R option> with
                        member self.DegreeOfParallelism = stream.DegreeOfParallelism
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> match chooser ctx value with Some value' -> resultRef := Some value'; cts.Cancel() | None -> ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            return! stream.Apply collectorf (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (stream : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun _ctx x -> chooser x) stream 

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPickLocal (chooser : 'T -> Local<'R option>) (stream : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun ctx x -> chooser x |> run ctx) stream 

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud stream evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (stream : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPick chooser stream 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud stream evaluates to None when the given function is applied.</exception>
    let inline pickLocal (chooser : 'T -> Local<'R option>) (stream : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPickLocal chooser stream 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (stream : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFind predicate stream 
            return 
                match result with
                | Some value -> true
                | None -> false
        }

    /// <summary>Tests if any element of the stream satisfies the given locally executing cloud predicate.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline existsLocal (predicate : 'T -> Local<bool>) (stream : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFindLocal predicate stream 
            return 
                match result with
                | Some value -> true
                | None -> false
        }


    /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (stream : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = exists (fun x -> not <| predicate x) stream
            return not result
        }


    /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forallLocal (predicate : 'T -> Local<bool>) (stream : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = existsLocal (fun x -> local { let! v = predicate x in return not v }) stream
            return not result
        }



    // Text ReadLine CloudFile producers

    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using text line reader.
    /// </summary>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFilesByLine (sources : seq<CloudFile>) : CloudFlow<string> =
        sources
        |> ofCloudFiles CloudFileReader.ReadLines
        |> collect id

    /// <summary>
    /// Constructs a CloudFlow of lines from a path.
    /// </summary>
    /// <param name="path">The path to the text file.</param>
    let ofTextFileByLine (path : string) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! fileSize = CloudFile.GetSize(path)
                    let! collector = collectorf 
                    let! workerCount = 
                        match collector.DegreeOfParallelism with
                        | Some n -> local { return n }
                        | _ -> Cloud.GetWorkerCount()
                    let! workers = Cloud.GetAvailableWorkers()

                    let rangeBasedReadLines (s : int64) (e : int64) (stream : System.IO.Stream) = 
                        seq {
                            let numOfBytesRead = ref 0L
                            stream.Seek(s, System.IO.SeekOrigin.Begin) |> ignore
                            let reader = new LineReader(stream)
                            let size = stream.Length
                            while s + !numOfBytesRead <= e && s + !numOfBytesRead < size do
                                let line = reader.ReadLine()
                                if s = 0L || !numOfBytesRead > 0L then
                                    yield line
                                numOfBytesRead := reader.NumOfBytesRead
                        }
                    let createTask (s : int64) (e : int64) (collector : Local<Collector<string, 'S>>) = 
                        local {
                            let! collector = collector
                            if s = e then
                                return! projection collector.Result
                            else
                                use! stream = CloudFile.Read(path, (fun stream -> async { return stream }), true)
                                let parStream = ParStream.ofSeq (rangeBasedReadLines s e stream) 
                                let collectorResult = parStream.Apply (toParStreamCollector collector)
                                return! projection collectorResult
                        }
                    if not (fileSize = 0L) then 
                        let partitions = Partitions.ofLongRange workerCount fileSize
                        let! results = 
                            partitions 
                            |> Array.mapi (fun i (s, e) -> (createTask s e collectorf, workers.[i % workers.Length])) 
                            |> Cloud.Parallel
                        return! combiner results 
                    else
                        return! projection collector.Result
                } }

