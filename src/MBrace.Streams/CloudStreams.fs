namespace MBrace.Streams
#nowarn "0444"

open System
open System.Threading
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq
open MBrace
open MBrace.Continuation
open Nessos.Streams
open Nessos.Streams.Internals

/// Represents a distributed Stream of values.
type CloudStream<'T> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option ref
    /// Applies the given collector to the CloudStream.
    abstract Apply<'S, 'R> : Cloud<Collector<'T, 'S>> -> ('S -> Cloud<'R>) -> ('R -> 'R -> 'R) -> Cloud<'R>

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudStreams.
module CloudStream =

    /// Maximum combined stream length used in ofCloudFiles.
    let private maxCloudFileCombinedLength = 1024L * 1024L * 1024L

    /// If local context then returns number of cores instead of throwing exception
    /// else returns number of workers.
    let inline private getWorkerCount() = cloud {
        let! ctx = Cloud.GetSchedulingContext()
        match ctx with
        | Sequential  -> return failwith "Invalid CloudStream context : %A" ctx
        | ThreadParallel -> return 1
        | Distributed -> return! Cloud.GetWorkerCount()
    }

    /// <summary>Wraps array as a CloudStream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudStream.</returns>
    let ofArray (source : 'T []) : CloudStream<'T> =
        let degreeOfParallelism = ref None
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = degreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud {
                    let! workerCount = 
                        match !self.DegreeOfParallelism with
                        | Some n -> cloud { return n }
                        | None -> getWorkerCount()

                    let createTask array (collector : Cloud<Collector<'T, 'S>>) = 
                        cloud {
                            let! collector = collector
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply collector
                            return! projection collectorResult
                        }
                    if not (source.Length = 0) then 
                        let partitions = Partitions.ofLongRange workerCount (int64 source.Length)
                        let! results = 
                            partitions 
                            |> Array.map (fun (s, e) -> createTask [| for i in s..(e - 1L) do yield source.[int i] |] collectorf) 
                            |> Cloud.Parallel
                        return Array.reduce combiner results
                    else
                        let! collector = collectorf
                        return! projection collector.Result
                } }

    /// <summary>
    /// Constructs a CloudStream from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFiles (reader : System.IO.Stream -> Async<'T>) (sources : seq<CloudFile>) : CloudStream<'T> =
        let degreeOfParallelism = ref None
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = degreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud { 
                    if Seq.isEmpty sources then 
                        let! collector = collectorf
                        return! projection collector.Result
                    else
                        let! workerCount = 
                            match !self.DegreeOfParallelism with
                            | Some n -> cloud { return n }
                            | None -> getWorkerCount()

                        let createTask (files : CloudFile []) (collectorf : Cloud<Collector<'T, 'S>>) : Cloud<'R> = 
                            cloud {
                                let rec partitionByLength (files : CloudFile []) index (currLength : int64) (currAcc : CloudFile list) (acc : CloudFile list list)=
                                    cloud {
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
                                let! resources = Cloud.GetResourceRegistry()
                                for fs in partitions do
                                    let! collector = collectorf
                                    let parStream = 
                                        fs
                                        |> ParStream.ofSeq 
                                        |> ParStream.map (fun file -> CloudFile.Read(file, reader, leaveOpen = true))
                                        |> ParStream.map (fun wf -> Cloud.RunSynchronously(wf, resources))
                                    let collectorResult = parStream.Apply collector
                                    let! partial = projection collectorResult
                                    result.Add(partial)
                                if result.Count = 0 then
                                    let! collector = collectorf
                                    return! projection collector.Result
                                else
                                    return Array.reduce combiner (result.ToArray())
                            }

                        let partitions = sources |> Seq.toArray |> Partitions.ofArray workerCount
                        let! results = partitions |> Array.map (fun cfiles -> createTask cfiles collectorf) |> Cloud.Parallel
                        return Array.reduce combiner results
                } }

    /// <summary>
    /// Constructs a CloudStream from a CloudArray.
    /// </summary>
    /// <param name="source">The input CloudArray.</param>
    let ofCloudArray (source : CloudArray<'T>) : CloudStream<'T> =
        let degreeOfParallelism = ref None
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = degreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) (combiner : 'R -> 'R -> 'R) =
                cloud {
                    let! workerCount = 
                        match !self.DegreeOfParallelism with
                        | Some n -> cloud { return n }
                        | None -> getWorkerCount()
                    
                    let createTask (partitionId : int) (collector : Cloud<Collector<'T, 'S>>) = 
                        cloud {
                            let! collector = collector
                            let! array = source.GetPartition(partitionId) 
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply collector
                            return! projection collectorResult
                        }

                    let createTaskCached (cached : CachedCloudArray<'T>) (taskId : string) (collectorf : Cloud<Collector<'T, 'S>>) = 
                        cloud { 
                            let partitions = CloudArrayCache.Get(cached, taskId) |> Seq.toArray
                            let completed = new ResizeArray<int * 'R>()
                            for pid in partitions do
                                let array = CloudArrayCache.GetPartition(cached, pid)
                                let parStream = ParStream.ofArray array
                                let! collector = collectorf
                                let collectorResult = parStream.Apply collector
                                let! partial = projection collectorResult
                                completed.Add(pid, partial)
                            return completed 
                        }
                    
                    if source.Length = 0L then
                        let! collector = collectorf
                        return! projection collector.Result;
                    else
                        let partitions = [|0..source.PartitionCount-1|]
                        match source with
                        | :? CachedCloudArray<'T> as cached -> 
                            // round 1
                            let taskId = Guid.NewGuid().ToString() //Cloud.GetTaskId()
                            let! partial = Cloud.Parallel(createTaskCached cached taskId collectorf)
                            let results1 = Seq.concat partial 
                                           |> Seq.toArray 
                            let completedPartitions = 
                                results1
                                |> Seq.map (fun (p, _) -> p)
                                |> Set.ofSeq
                            let allPartitions = partitions |> Set.ofSeq
                            // round 2
                            let restPartitions = allPartitions - completedPartitions
                            
                            if Seq.isEmpty restPartitions then
                                let final = results1 
                                            |> Seq.sortBy (fun (p,_) -> p)
                                            |> Seq.map (fun (_,r) -> r) 
                                            |> Seq.toArray
                                return Array.reduce combiner final
                            else
                                let! results2 = restPartitions 
                                                |> Set.toArray 
                                                |> Array.map (fun pid -> cloud { let! r = createTask pid collectorf in return pid,r }) 
                                                |> Cloud.Parallel
                                let final = Seq.append results1 results2
                                            |> Seq.sortBy (fun (p,_) -> p)
                                            |> Seq.map (fun (_,r) -> r)
                                            |> Seq.toArray
                                return Array.reduce combiner final
                        | source -> 
                            let! results = partitions |> Array.map (fun partitionId -> createTask partitionId collectorf) |> Cloud.Parallel
                            return Array.reduce combiner results
                            
                } }

    /// <summary>
    /// Returns a cached version of the given CloudArray.
    /// </summary>
    /// <param name="source">The input CloudArray.</param>
    let cache (source : CloudArray<'T>) : Cloud<CloudArray<'T>> = 
        cloud {
            let createTask (pid : int) (cached : CachedCloudArray<'T>) = 
                cloud {
                    let! slice = (cached :> CloudArray<'T>).GetPartition(pid)
                    CloudArrayCache.Add(cached, pid, slice)
                }
            let taskId = Guid.NewGuid().ToString()
            let cached = new CachedCloudArray<'T>(source, taskId)
            if source.Length > 0L then
                let partitions = [|0..source.PartitionCount-1|]
                do! partitions 
                    |> Array.map (fun pid -> createTask pid cached) 
                    |> Cloud.Parallel
                    |> Cloud.Ignore
            return cached :> _
        }

    // intermediate functions

    /// <summary>Transforms each element of the input CloudStream.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Cloud<Collector<'R, 'S>>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' = cloud {
                    let! collector = collectorf
                    return 
                      { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = (fun value -> iter (f value));
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudStream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline flatMap (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Cloud<Collector<'R, 'S>>) (projection : 'S -> Cloud<'Result>) combiner =
                let collectorf' = cloud {
                    let! collector = collectorf
                    return 
                      { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = 
                                    (fun value -> 
                                        let (Stream streamf) = f value
                                        let cts = CancellationTokenSource.CreateLinkedTokenSource(iterator.Cts.Token)
                                        let { Bulk = bulk; Iterator = _ } = streamf { Complete = (fun () -> ()); Cont = iter; Cts = cts } in bulk ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                stream.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudStream to a new stream and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline collect (f : 'T -> Stream<'R>) (stream : CloudStream<'T>) : CloudStream<'R> =
        flatMap f stream

    /// <summary>Filters the elements of the input CloudStream.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline filter (predicate : 'T -> bool) (stream : CloudStream<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) combiner =
                let collectorf' = cloud {
                    let! collector = collectorf
                    return { new Collector<'T, 'S> with
                        member self.Iterator() = 
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Index = iterator.Index; 
                                Func = (fun value -> if predicate value then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                stream.Apply collectorf' projection combiner }


    /// <summary>Returns a cloud stream with a new degree of parallelism.</summary>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The result cloud stream.</returns>
    let inline withDegreeOfParallelism (degreeOfParallelism : int) (stream : CloudStream<'T>) : CloudStream<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else
            stream.DegreeOfParallelism := Some degreeOfParallelism
            { new CloudStream<'T> with
                    member self.DegreeOfParallelism = stream.DegreeOfParallelism
                    member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) combiner =
                        stream.Apply collectorf projection combiner }

    // terminal functions

    /// <summary>Applies a function to each element of the CloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : CloudStream<'T>) : Cloud<'State> =
        let collectorf = cloud {  
            let results = new List<'State ref>()
            return
              { new Collector<'T, 'State> with
                member self.Iterator() = 
                    let accRef = ref <| state ()
                    results.Add(accRef)
                    {   Index = ref -1;
                        Func = (fun value -> accRef := folder !accRef value);
                        Cts = new CancellationTokenSource() }
                member self.Result = 
                    let mutable acc = state ()
                    for result in results do
                            acc <- combiner acc !result 
                    acc }
        }
        stream.Apply collectorf (fun x -> cloud { return x }) combiner

    /// <summary>Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input CloudStream to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (stream : CloudStream<'T>) : CloudStream<'Key * 'State> = 
        let collectorf (totalWorkers : int) = cloud {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.Iterator() = 
                    {   Index = ref -1; 
                        Func =
                            (fun value -> 
                                    let mutable grouping = Unchecked.defaultof<_>
                                    let key = projection value
                                    if dict.TryGetValue(key, &grouping) then
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder !acc value) 
                                    else
                                        grouping <- ref <| state ()
                                        if not <| dict.TryAdd(key, grouping) then
                                            dict.TryGetValue(key, &grouping) |> ignore
                                        let acc = grouping
                                        lock grouping (fun () -> acc := folder !acc value) 
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
                let combiner' (left : CloudArray<_>) (right : CloudArray<_>) = 
                    left.Append(right)
                let! totalWorkers = match !stream.DegreeOfParallelism with Some n -> cloud { return n } | None -> getWorkerCount()
                let! keyValueArray = stream.Apply (collectorf totalWorkers) 
                                                  (fun keyValues -> cloud {
                                                        let dict = new Dictionary<int, CloudArray<'Key * 'State>>() 
                                                        for (key, value) in keyValues do
                                                            let! values = CloudArray.New(value)
                                                            dict.[key] <- values
                                                        let values = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value))
                                                        return! CloudArray.New(values) }) combiner'
                
                let! kva = keyValueArray.ToEnumerable()
                let dict = 
                    let dict = new Dictionary<int, CloudArray<'Key * 'State>>()
                    for (key, value) in kva do
                        let mutable grouping = Unchecked.defaultof<_>
                        if dict.TryGetValue(key, &grouping) then
                            dict.[key] <- grouping.Append(value)
                        else
                             dict.[key] <- value
                    dict
                let keyValues = dict |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value)) |> Seq.toArray 
                return keyValues
            }
        let reducerf = cloud {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! resources = Cloud.GetResourceRegistry()
            return { new Collector<int * CloudArray<'Key * 'State>,  seq<'Key * 'State>> with
                member self.Iterator() = 
                    {   Index = ref -1; 
                        Func =
                            (fun (_, keyValues) ->
                                let keyValues = Cloud.RunSynchronously(keyValues.ToEnumerable(), resources)
                                   
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
                        Cts = new CancellationTokenSource() }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }
        // Phase 2
        let reducer (stream : CloudStream<int * CloudArray<'Key * 'State>>) : Cloud<CloudArray<'Key * 'State>> = 
            cloud {
                let combiner' (left : CloudArray<_>) (right : CloudArray<_>) = 
                    left.Append(right)

                let! keyValueArray = stream.Apply reducerf (fun keyValues -> CloudArray.New(keyValues)) combiner'
                return keyValueArray
            }
        { new CloudStream<'Key * 'State> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'Key * 'State, 'S>>) (projection : 'S -> Cloud<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (ofArray result)
                    return! (ofCloudArray result').Apply collectorf projection combiner
                }  }


    /// <summary>
    /// Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudStream to keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : CloudStream<'T>) : CloudStream<'Key * int64> =
        foldBy projection (fun state _ -> state + 1L) (+) (fun () -> 0L) stream

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (stream : CloudStream< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) stream

    /// <summary>Returns the total number of elements of the CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (stream : CloudStream<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) stream

    /// <summary>Creates an array from the given CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (stream : CloudStream<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) stream 
            return arrayCollector.ToArray()
        }

    /// <summary>Creates a CloudArray from the given CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudArray.</returns>    
    let inline toCloudArray (stream : CloudStream<'T>) : Cloud<CloudArray<'T>> =
        let collectorf = cloud { 
            let results = new List<List<'T>>()
            return 
              { new Collector<'T, 'T []> with
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
        stream.Apply collectorf (fun array -> cloud { 
                                                let! processId = Cloud.GetProcessId() 
                                                return! CloudArray.New(array) }) 
                                (fun left right -> left.Append(right))

    /// <summary>Applies a key-generating function to each element of the input CloudStream and yields the CloudStream of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudStream into comparable keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudStream.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (stream : CloudStream<'T>) : CloudStream<'T> = 
        let collectorf = cloud {  
            let results = new List<List<'T>>()
            return 
              { new Collector<'T, List<'Key[] * 'T []>> with
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
                            keys.[counter] <- projection value
                            values.[counter] <- value
                    Sort.parallelSort Environment.ProcessorCount keys values
                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(), 
                                     values.Take(takeCount).ToArray())) }
        }
        let sortByComp = 
            cloud {
                let! results = stream.Apply collectorf (fun x -> cloud { return x }) (fun left right -> left.AddRange(right); left)
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
                    Sort.parallelSort Environment.ProcessorCount keys values    
                    values.Take(takeCount).ToArray()
                return result
            }
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Cloud<Collector<'T, 'S>>) (projection : 'S -> Cloud<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (ofArray result).Apply collectorf projection combiner
                }  
        }


