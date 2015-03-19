namespace MBrace.Streams

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
type CloudStream<'T> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option
    /// Applies the given collector to the CloudStream.
    abstract Apply<'S, 'R> : Local<Collector<'T, 'S>> -> ('S -> Local<'R>) -> ('R []  -> Local<'R>) -> Cloud<'R>

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudStreams.
module CloudStream =

    //#region Helpers

    /// Maximum combined stream length used in ofCloudFiles.
    let private maxCloudFileCombinedLength = 1024L * 1024L * 1024L
    
    /// Maximum CloudVector partition size used in CloudVector.New.
    [<Literal>]
    let private maxCloudVectorPartitionSize = 1073741824L // 1GB


    /// gets all partition indices found in cloud vector
    let inline private getPartitionIndices (v : CloudVector<'T>) = [| 0 .. v.PartitionCount - 1 |]

    /// Flat map/reduce with sequential execution on leafs.
    let inline private parallelInChunks (npar : int) (workflows : Local<'T> []) = cloud {
        let! partials = 
            workflows 
            |> Partitions.ofArray npar
            |> Array.map (fun wfs -> wfs |> Local.Parallel)
            |> Cloud.Parallel
        return Array.concat partials
    }

    /// Converts MBrace.Streams.Collector to Nessos.Streams.Collector
    let inline private toParStreamCollector (collector : Collector<'T, 'S>) =
        { new Nessos.Streams.Collector<'T, 'S> with
            member self.DegreeOfParallelism = match collector.DegreeOfParallelism with Some n -> n | None -> Environment.ProcessorCount
            member self.Iterator() = collector.Iterator()
            member self.Result = collector.Result  }

    //#endregion

    //#region Driver functions

    /// <summary>Wraps array as a CloudStream.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudStream.</returns>
    let ofArray (source : 'T []) : CloudStream<'T> =
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! collector = collectorf 
                    let! workerCount = 
                        match collector.DegreeOfParallelism with
                        | Some n -> local { return n }
                        | _ -> Cloud.GetWorkerCount()

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
                            |> Array.map (fun (s, e) -> createTask [| for i in s..(e - 1L) do yield source.[int i] |] collectorf) 
                            |> Cloud.Parallel
                        return! combiner results 
                    else
                        return! projection collector.Result
                } }

    /// <summary>
    /// Constructs a CloudStream from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to an object.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFiles (reader : System.IO.Stream -> Async<'T>) (sources : seq<CloudFile>) : CloudStream<'T> =
        { new CloudStream<'T> with
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
                        let! results = partitions |> Array.map (fun cfiles -> createTask cfiles collectorf) |> Cloud.Parallel
                        return! combiner results
                } }

    /// <summary>
    /// Constructs a CloudStream from a CloudVector.
    /// </summary>
    /// <param name="source">The input CloudVector.</param>
    let ofCloudVector (source : CloudVector<'T>) : CloudStream<'T> =
        { new CloudStream<'T> with
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

                        // TODO : need a scheduling algorithm to assign partition indices
                        //        to workers according to current cache state.
                        //        for now blindly assign partitions among workers.

                        let! results =
                            partitions
                            |> Partitions.ofArray workerCount
                            |> Seq.filter (not << Array.isEmpty)
                            |> Seq.map computePartitions 
                            |> Cloud.Parallel

                        return! combiner results
                }
        }

    //#endregion

    //#region Intermediate functions

    /// <summary>Transforms each element of the input CloudStream.</summary>
    /// <param name="f">A function to transform items from the input CloudStream.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudStream.</returns>
    let inline map (f : 'T -> 'R) (stream : CloudStream<'T>) : CloudStream<'R> =
        { new CloudStream<'R> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return 
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
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
            member self.Apply<'S, 'Result> (collectorf : Local<Collector<'R, 'S>>) (projection : 'S -> Local<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return 
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
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
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
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
            { new CloudStream<'T> with
                    member self.DegreeOfParallelism = Some degreeOfParallelism
                    member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                        stream.Apply collectorf projection combiner }

    // terminal functions


    /// <summary>Applies a local cloud function to each element of the CloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A local cloud function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A local cloud function that combines partial states into a new state.</param>
    /// <param name="state">A local cloud function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline foldLocal (folder : 'State -> 'T -> Local<'State>) (combiner : 'State -> 'State -> Local<'State>) 
                         (state : unit -> Local<'State>) (stream : CloudStream<'T>) : Cloud<'State> =
        let collectorf = local {  
            let results = new List<'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let run a = Async.RunSync(Cloud.ToAsync(a, ctx.Resources,ctx.CancellationToken))
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                member self.Iterator() = 
                    let accRef = ref <| run (state ())
                    results.Add(accRef)
                    {   Index = ref -1;
                        Func = (fun value -> accRef := run (folder !accRef value));
                        Cts = new CancellationTokenSource() }
                member self.Result = 
                    let mutable acc = run (state ())
                    for result in results do
                            acc <- run (combiner acc !result)
                    acc }
        }
        stream.Apply collectorf (fun x -> local { return x }) (fun values -> local { return Array.reduce combiner values })

    /// <summary>Applies a function to each element of the CloudStream, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudStream.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (stream : CloudStream<'T>) : Cloud<'State> =
        let collectorf = local {  
            let results = new List<'State ref>()
            return
              { new Collector<'T, 'State> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
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
        stream.Apply collectorf (fun x -> local { return x }) (fun values -> local { return Array.reduce combiner values })

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
        let collectorf (totalWorkers : int) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            return
              { new Collector<'T,  seq<int * seq<'Key * 'State>>> with
                member self.DegreeOfParallelism = stream.DegreeOfParallelism 
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
        let reducer (stream : CloudStream<int * CloudVector<'Key * 'State>>) : Cloud<CloudVector<'Key * 'State>> = 
            cloud {
                let combiner' (result : CloudVector<_> []) = local { return CloudVector.Merge result }

                let! keyValueArray = stream.Apply reducerf (fun keyValues -> CloudVector.New(keyValues, maxCloudVectorPartitionSize)) combiner'
                return keyValueArray
            }
        { new CloudStream<'Key * 'State> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'Key * 'State, 'S>>) (projection : 'S -> Local<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (ofArray result)
                    return! (ofCloudVector result').Apply collectorf projection combiner
                }  }


    /// <summary>
    /// Applies a key-generating function to each element of a CloudStream and return a CloudStream yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudStream to keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    let inline countBy (projection : 'T -> 'Key) (stream : CloudStream<'T>) : CloudStream<'Key * int64> =
        foldBy projection (fun state _ -> state + 1L) (+) (fun () -> 0L) stream

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>Nothing.</returns>
    let iter (action: 'T -> unit) (stream : CloudStream< 'T >) : Cloud< unit > =
        fold (fun () x -> action x) (fun () () -> ()) (fun () -> ()) stream

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

    /// <summary>Creates a CloudVector from the given CloudStream.</summary>
    /// <param name="stream">The input CloudStream.</param>
    /// <returns>The result CloudVector.</returns>    
    let inline toCloudVector (stream : CloudStream<'T>) : Cloud<CloudVector<'T>> =
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

    /// <summary>Applies a key-generating function to each element of the input CloudStream and yields the CloudStream of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudStream into comparable keys.</param>
    /// <param name="stream">The input CloudStream.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudStream.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (stream : CloudStream<'T>) : CloudStream<'T> = 
        let collectorf = local {  
            let results = new List<List<'T>>()
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
                            keys.[counter] <- projection value
                            values.[counter] <- value
                    if System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values)
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
                    if System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    values.Take(takeCount).ToArray()
                return result
            }
        { new CloudStream<'T> with
            member self.DegreeOfParallelism = stream.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (ofArray result).Apply collectorf projection combiner
                }  
        }



    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (stream : CloudStream<'T>) : Cloud<'T option> =
        let collectorf = 
            local {
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts =  new CancellationTokenSource()
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = stream.DegreeOfParallelism 
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> if predicate value then resultRef := Some value; cts.Cancel() else ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            return! stream.Apply collectorf (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud stream.</exception>
    let inline find (predicate : 'T -> bool) (stream : CloudStream<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFind predicate stream 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (stream : CloudStream<'T>) : Cloud<'R option> = 
        
        let collectorf = 
            local {
                let resultRef = ref Unchecked.defaultof<'R option>
                let cts = new CancellationTokenSource()
                return 
                    { new Collector<'T, 'R option> with
                        member self.DegreeOfParallelism = stream.DegreeOfParallelism
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> match chooser value with Some value' -> resultRef := Some value'; cts.Cancel() | None -> ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            return! stream.Apply collectorf (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud stream evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud stream evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (stream : CloudStream<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPick chooser stream 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Tests if any element of the stream satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (stream : CloudStream<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFind predicate stream 
            return 
                match result with
                | Some value -> true
                | None -> false
        }


    /// <summary>Tests if all elements of the parallel stream satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="stream">The input cloud stream.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (stream : CloudStream<'T>) : Cloud<bool> = 
        cloud {
            let! result = exists (fun x -> not <| predicate x) stream
            return not result
        }



    // Text ReadLine CloudFile producers

    /// <summary>
    /// Constructs a CloudStream from a collection of CloudFiles using text line reader.
    /// </summary>
    /// <param name="sources">The collection of CloudFiles.</param>
    let ofCloudFilesByLine (sources : seq<CloudFile>) : CloudStream<string> =
        sources
        |> ofCloudFiles CloudFileReader.ReadLines
        |> flatMap Stream.ofSeq

    /// <summary>
    /// Constructs a CloudStream of lines from a path.
    /// </summary>
    /// <param name="path">The path to the text file.</param>
    let ofTextFileByLine (path : string) : CloudStream<string> =
        { new CloudStream<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! fileSize = CloudFile.GetSize(path)
                    let! collector = collectorf 
                    let! workerCount = 
                        match collector.DegreeOfParallelism with
                        | Some n -> local { return n }
                        | _ -> Cloud.GetWorkerCount()

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
                            |> Array.map (fun (s, e) -> createTask s e collectorf) 
                            |> Cloud.Parallel
                        return! combiner results 
                    else
                        return! projection collector.Result
                } }

