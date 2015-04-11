namespace MBrace.Flow

#nowarn "0443"
#nowarn "0444"

open System
open System.Threading
open System.Text
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq
open MBrace
open MBrace.Store
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Flow.Internals
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

/// Provides CloudFlow producers.
type CloudFlow =
    /// Maximum combined flow length used in ofCloudFiles.
    static member internal maxCloudFileCombinedLength = 1024L * 1024L * 1024L
    
    /// Maximum CloudVector partition size used in CloudVector.New.
    static member internal maxCloudVectorPartitionSize = 1073741824L // 1GB

    /// gets all partition indices found in cloud vector
    static member internal getPartitionIndices (v : CloudVector<'T>) = [| 0 .. v.PartitionCount - 1 |]

    /// Converts MBrace.Flow.Collector to Nessos.Streams.Collector
    static member internal toParStreamCollector (collector : Collector<'T, 'S>) =
        { new Nessos.Streams.Collector<'T, 'S> with
            member self.DegreeOfParallelism = match collector.DegreeOfParallelism with Some n -> n | None -> Environment.ProcessorCount
            member self.Iterator() = collector.Iterator()
            member self.Result = collector.Result  }

    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    static member ofArray (source : 'T []) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! collector = collectorf
                    let! workers = Cloud.GetAvailableWorkers() 
                    let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                    let workerCount = defaultArg collector.DegreeOfParallelism workers.Length

                    let createTask array (collector : Local<Collector<'T, 'S>>) = 
                        local {
                            let! collector = collector
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply (CloudFlow.toParStreamCollector collector)
                            return! projection collectorResult
                        }
                    if source.Length > 0 then 
                        let partitions = Partitions.ofLongRange workerCount (int64 source.Length)
                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! results = 
                            if targetedworkerSupport then
                                partitions 
                                |> Array.mapi (fun i (s, e) -> 
                                                    let cloudBlock = createTask source.[int s .. int e - 1] collectorf
                                                    (cloudBlock, workers.[i % workers.Length])) 
                                |> Cloud.Parallel
                            else
                                partitions 
                                |> Array.map (fun (s, e) -> createTask source.[int s .. int e - 1] collectorf)
                                |> Cloud.Parallel
                        return! combiner results 
                    else
                        return! projection collector.Result
                } }

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    static member ofCloudCollection (collection : ICloudCollection<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    // TODO: 
                    //  1. take nested partitioning into account
                    //  2. reconcile degreeOfParallelism with partition grouping and partition.Size
                    // these actions will allow for integration with ofCloudFile and an efficient implementation of 'ofTextFilesByLines'.

                    let! collector = collectorf
                    let! workers = Cloud.GetAvailableWorkers()
                    let workers = workers |> Array.sortBy (fun w -> w.Id)
                    let degreeOfParallelism = defaultArg collector.DegreeOfParallelism workers.Length
                    // detect partitions for collection, if any
                    let! partitions = local {
                        match collection with
                        | :? IPartitionedCollection<'T> as pcc -> let! partitions = pcc.GetPartitions() in return Some partitions
                        | :? IPartitionableCollection<'T> as pcc -> let! partitions = pcc.GetPartitions degreeOfParallelism in return Some partitions
                        | _ -> return None
                    }

                    let tryGetCachedContents (collection : ICloudCollection<'T>) = local {
                        match box collection with
                        | :? Store.ICloudCacheable<'T []> as cc -> return! CloudCache.TryGetCachedValue cc
                        | _ -> return None
                    }

                    match partitions with
                    | None ->
                        // no partition scheme, materialize collection in-memory and offload to CloudFlow.ofArray
                        let! cached = tryGetCachedContents collection
                        let! array = local {
                            match cached with
                            | Some c -> return c
                            | None ->
                                let! enum = collection.ToEnumerable()
                                return Seq.toArray enum
                        }

                        let arrayFlow = CloudFlow.ofArray array
                        return! arrayFlow.Apply collectorf projection combiner

                    | Some partitions ->
                        // partition scheme exists, use for parallel reduction
                        let createTask (partition : ICloudCollection<'T>) = local {
                            let! collector = collectorf
                            let! parStream = local {
                                let! cached = tryGetCachedContents partition
                                match cached with
                                | Some c -> return ParStream.ofArray c
                                | None ->
                                    let! enum = partition.ToEnumerable()
                                    return ParStream.ofSeq enum
                            }

                            let collectorResult = parStream.Apply (CloudFlow.toParStreamCollector collector)
                            return! projection collectorResult
                        }

                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! results =
                            if targetedworkerSupport then
                                partitions
                                |> Seq.mapi (fun i partition -> (createTask partition, workers.[i % workers.Length]))
                                |> Cloud.Parallel
                            else
                                partitions
                                |> Seq.map createTask
                                |> Cloud.Parallel
                        return! combiner results
                }
        }


    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    static member ofCloudFiles (reader : System.IO.Stream -> Async<'T>) (sources : seq<string>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud { 
                    if Seq.isEmpty sources then 
                        let! collector = collectorf
                        return! projection collector.Result
                    else
                        let! collector = collectorf
                        let! workers = Cloud.GetAvailableWorkers() 
                        let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                        let workerCount = defaultArg collector.DegreeOfParallelism workers.Length

                        let createTask (files : CloudFile []) (collectorf : Local<Collector<'T, 'S>>) : Local<'R> = 
                            local {
                                // create file partitions according to accumulated size; this is to restrict IO in every posted job
                                let rec partitionByLength (files : CloudFile []) index (currLength : int64) (currAcc : CloudFile list) (acc : CloudFile list list)=
                                    local {
                                        if index >= files.Length then return (currAcc :: acc) |> List.filter (not << List.isEmpty)
                                        else
                                            let! length = files.[index].Size
                                            if length >= CloudFlow.maxCloudFileCombinedLength then
                                                return! partitionByLength files (index + 1) length [files.[index]] (currAcc :: acc)
                                            elif length + currLength >= CloudFlow.maxCloudFileCombinedLength then
                                                return! partitionByLength files index 0L [] (currAcc :: acc)
                                            else
                                                return! partitionByLength files (index + 1) (currLength + length) (files.[index] :: currAcc) acc
                                    }
                                let! partitions = partitionByLength files 0 0L [] []

                                let result = new ResizeArray<'R>(partitions.Length)
                                let! ctx = Cloud.GetExecutionContext()
                                // sequentially process partitions grouped by accumulated size
                                for fs in partitions do
                                    let! collector = collectorf
                                    let parStream = 
                                        fs
                                        |> ParStream.ofSeq 
                                        |> ParStream.map (fun file -> CloudFile.Read(file.Path, reader, leaveOpen = true))
                                        |> ParStream.map (fun wf -> Cloud.RunSynchronously(wf, ctx.Resources, ctx.CancellationToken))
                                    let collectorResult = parStream.Apply (CloudFlow.toParStreamCollector collector)
                                    let! partial = projection collectorResult
                                    result.Add(partial)
                                if result.Count = 0 then
                                    let! collector = collectorf
                                    return! projection collector.Result
                                else
                                    return! combiner (result.ToArray())
                            }

                        let partitions = sources |> Seq.map (fun p -> new CloudFile(p)) |> Seq.toArray |> Partitions.ofArray workerCount
                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! results = 
                            if targetedworkerSupport then
                                partitions 
                                |> Array.mapi (fun i cfiles -> (createTask cfiles collectorf, workers.[i % workers.Length])) 
                                |> Cloud.Parallel
                            else
                                partitions 
                                |> Array.map (fun cfiles -> createTask cfiles collectorf) 
                                |> Cloud.Parallel
                        return! combiner results
                } }

    /// <summary>
    /// Constructs a CloudFlow from a CloudVector.
    /// </summary>
    /// <param name="source">The input CloudVector.</param>
    static member ofCloudVector (source : CloudVector<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let useCache = source.IsCachingSupported
                    let partitions = CloudFlow.getPartitionIndices source


                    let computePartitions (partitions : int []) = local {

                        let computePartition (pIndex : int) = local {
                            let partition = source.GetPartition pIndex
                            let! collector = collectorf
                            if useCache then do! partition.ForceCache() |> Local.Ignore
                            let! array = partition.ToArray()
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply (CloudFlow.toParStreamCollector collector)
                            return! projection collectorResult
                        }

                        /// use sequential computation; should probably allow some degree of parallelization
                        let! results = Sequential.map computePartition partitions

                        // do not allow cache state updating in flow execution under local runtimes
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
                        let! workers = Cloud.GetAvailableWorkers() 
                        let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                        let workerCount = defaultArg collector.DegreeOfParallelism workers.Length


                        // TODO : need a scheduling algorithm to assign partition indices
                        //        to workers according to current cache state.
                        //        for now blindly assign partitions among workers.
                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! results =
                            if targetedworkerSupport then
                                partitions
                                |> Partitions.ofArray workerCount
                                |> Seq.filter (not << Array.isEmpty)
                                |> Seq.mapi (fun i partitions -> (computePartitions partitions, workers.[i % workers.Length]))
                                |> Cloud.Parallel
                            else
                                partitions
                                |> Partitions.ofArray workerCount
                                |> Seq.filter (not << Array.isEmpty)
                                |> Seq.map (fun partitions -> computePartitions partitions)
                                |> Cloud.Parallel
                        return! combiner results
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow of lines from a single large text file.
    /// </summary>
    /// <param name="path">The path to the text file.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member ofTextFileByLine (path : string, ?encoding : Encoding) : CloudFlow<string> = 
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! cseq = CloudSequence.FromLineSeparatedTextFile(path, ?encoding = encoding, enableCache = false, force = false)
                let collectionStream = CloudFlow.ofCloudCollection cseq
                return! collectionStream.Apply collectorf projection combiner
            }  
        }

//    /// <summary>
//    ///     Constructs a CloudFlow of lines from a collection of text files.
//    /// </summary>
//    /// <param name="paths">Paths to the text files.</param>
//    /// <param name="encoding">Optional encoding.</param>
//    static member ofTextFilesByLine (paths : seq<string>, ?encoding : Encoding) : CloudFlow<string> =
//        { new CloudFlow<string> with
//            member self.DegreeOfParallelism = None
//            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
//                let flow = CloudFlow.ofCloudFiles (fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding)) paths
//                return! flow.Apply collectorf projection combiner
//            }
//        }
                
    /// <summary>Creates a CloudFlow from the ReceivePort of a CloudChannel</summary>
    /// <param name="channel">the ReceivePort of a CloudChannel.</param>
    /// <param name="degreeOfParallelism">The number of concurrently receiving tasks</param>
    /// <returns>The result CloudFlow.</returns>
    static member ofCloudChannel (channel : IReceivePort<'T>, degreeOfParallelism : int) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = Some degreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! collector = collectorf 
                    let! workers = Cloud.GetAvailableWorkers() 
                    let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                    let workerCount = defaultArg collector.DegreeOfParallelism workers.Length

                    let createTask (collector : Local<Collector<'T, 'S>>) = 
                        local {
                            let! ctx = Cloud.GetExecutionContext()
                            let! collectorf = collector
                            let seq = Seq.initInfinite (fun _ -> Cloud.RunSynchronously(CloudChannel.Receive channel, ctx.Resources, ctx.CancellationToken))
                            let parStream = ParStream.ofSeq seq
                            let collectorResult = parStream.Apply (CloudFlow.toParStreamCollector collectorf)
                            return! projection collectorResult
                        }
                    
                    let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                    let! results = 
                        if targetedworkerSupport then
                            [|1..workerCount|] 
                            |> Array.map (fun i -> (createTask collectorf, workers.[i % workers.Length])) 
                            |> Cloud.Parallel
                        else
                            [|1..workerCount|] 
                            |> Array.map (fun i -> createTask collectorf)
                            |> Cloud.Parallel

                    return! combiner results

                } }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
/// Provides basic operations on CloudFlows.
module CloudFlow =

    //#region Intermediate functions

    let inline private run ctx a = Cloud.RunSynchronously(a, ctx.Resources,ctx.CancellationToken)

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline private mapGen  (f : ExecutionContext -> 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline map  (f : 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun ctx x -> f x) flow

    /// <summary>Transforms each element of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline mapLocal (f : 'T -> Local<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        mapGen (fun ctx x -> f x |> run ctx) flow

    let inline private collectGen (f : ExecutionContext -> 'T -> seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collect (f : 'T -> seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun ctx x -> f x) flow 

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements using a locally executing cloud function.</summary>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline collectLocal (f : 'T -> Local<seq<'R>>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        collectGen (fun ctx x -> f x |> run ctx) flow 

    let inline private filterGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
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
                flow.Apply collectorf' projection combiner }

    /// <summary>Filters the elements of the input CloudFlow.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filter (predicate : 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun ctx x -> predicate x) flow 

    /// <summary>Filters the elements of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let inline filterLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        filterGen (fun ctx x -> predicate x |> run ctx) flow 

    /// <summary>Returns a cloud flow with a new degree of parallelism.</summary>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The result cloud flow.</returns>
    let inline withDegreeOfParallelism (degreeOfParallelism : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        if degreeOfParallelism < 1 then
            raise <| new ArgumentOutOfRangeException("degreeOfParallelism")
        else
            { new CloudFlow<'T> with
                    member self.DegreeOfParallelism = Some degreeOfParallelism
                    member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner =
                        flow.Apply collectorf projection combiner }

    // terminal functions

    let inline private foldGen (folder : ExecutionContext -> 'State -> 'T -> 'State) (combiner : ExecutionContext -> 'State -> 'State -> 'State) 
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
                flow.Apply 
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



    /// <summary>Applies a locally executing cloud function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline foldLocal (folder : 'State -> 'T -> Local<'State>) (combiner : 'State -> 'State -> Local<'State>) 
                         (state : unit -> Local<'State>) (flow : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun ctx x y -> run ctx (folder x y)) (fun ctx x y -> run ctx (combiner x y)) (fun ctx -> run ctx (state ())) flow

    /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let inline fold (folder : 'State -> 'T -> 'State) (combiner : 'State -> 'State -> 'State) 
                    (state : unit -> 'State) (flow : CloudFlow<'T>) : Cloud<'State> =
        foldGen (fun ctx x y -> folder x y) (fun ctx x y -> combiner x y) (fun ctx -> state ()) flow

    let inline private foldByGen (projection : ExecutionContext -> 'T -> 'Key) 
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
                                     |> Seq.groupBy (fun keyValue -> Math.Abs(keyValue.Key.GetHashCode()) % totalWorkers)
                                     |> Seq.map (fun (key, keyValues) -> (key, keyValues |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value))))
                    partitions }
        }
        // Phase 1
        let shuffling = 
            cloud {
                let combiner' (result : _ []) = local { return Array.concat result }
                let! totalWorkers = match flow.DegreeOfParallelism with Some n -> local { return n } | None -> Cloud.GetWorkerCount()
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.Apply (collectorf cts totalWorkers) 
                                                  (fun keyValues -> local {
                                                        let dict = new Dictionary<int, CloudVector<'Key * 'State>>() 
                                                        for (key, value) in keyValues do
                                                            let! values = CloudVector.New(value, CloudFlow.maxCloudVectorPartitionSize, enableCaching = false)
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
        let reducerf (cloudCts : ICloudCancellationTokenSource) = local {
            let dict = new ConcurrentDictionary<'Key, 'State ref>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return { new Collector<int * CloudVector<'Key * 'State>,  seq<'Key * 'State>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
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
                        Cts = cts }
                member self.Result =
                    dict
                    |> Seq.map (fun keyValue -> (keyValue.Key, !keyValue.Value)) }
        }
        // Phase 2
        let reducer (flow : CloudFlow<int * CloudVector<'Key * 'State>>) : Cloud<CloudVector<'Key * 'State>> = 
            cloud {
                let combiner' (result : CloudVector<_> []) = local { return CloudVector.Merge result }
                let! cts = Cloud.CreateCancellationTokenSource()
                let! keyValueArray = flow.Apply (reducerf cts) (fun keyValues -> CloudVector.New(keyValues, CloudFlow.maxCloudVectorPartitionSize)) combiner'
                return keyValueArray
            }
        { new CloudFlow<'Key * 'State> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'Key * 'State, 'S>>) (projection : 'S -> Local<'R>) combiner =
                cloud {
                    let! result = shuffling
                    let! result' = reducer (CloudFlow.ofArray result)
                    return! (CloudFlow.ofCloudVector result').Apply collectorf projection combiner
                }  }


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator. The folder, combiner and state are locally executing cloud functions.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let foldByLocal (projection : 'T -> Local<'Key>) 
                           (folder : 'State -> 'T -> Local<'State>) 
                           (combiner : 'State -> 'State -> Local<'State>) 
                           (state : unit -> Local<'State>) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun ctx x -> projection x |> run ctx) (fun ctx x y -> folder x y |> run ctx) (fun ctx s1 s2 -> combiner s1 s2 |> run ctx) (fun ctx -> state () |> run ctx) flow 

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The final result.</returns>
    let foldBy (projection : 'T -> 'Key) 
                      (folder : 'State -> 'T -> 'State) 
                      (combiner : 'State -> 'State -> 'State) 
                      (state : unit -> 'State) (flow : CloudFlow<'T>) : CloudFlow<'Key * 'State> = 
        foldByGen (fun ctx x -> projection x) (fun ctx x y -> folder x y) (fun ctx s1 s2 -> combiner s1 s2) (fun ctx -> state ()) flow 

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    let countBy (projection : 'T -> 'Key) (flow : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun _ctx x -> projection x) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun _ctx -> 0L) flow

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    let countByLocal (projection : 'T -> Local<'Key>) (flow : CloudFlow<'T>) : CloudFlow<'Key * int64> =
        foldByGen (fun ctx x -> projection x |> run ctx) (fun _ctx state _ -> state + 1L) (fun _ctx x y -> x + y) (fun ctx -> 0L) flow

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iter (action: 'T -> unit) (flow : CloudFlow< 'T >) : Cloud< unit > =
        fold (fun () x -> action x) (fun () () -> ()) (fun () -> ()) flow

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let iterLocal (action: 'T -> Local<unit>) (flow : CloudFlow< 'T >) : Cloud< unit > =
        foldLocal (fun () x -> action x) (fun () () -> local { return () }) (fun () -> local { return () }) flow

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the elements.</returns>
    let inline sum (flow : CloudFlow< ^T >) : Cloud< ^T > 
            when ^T : (static member ( + ) : ^T * ^T -> ^T) 
            and  ^T : (static member Zero : ^T) = 
        fold (+) (+) (fun () -> LanguagePrimitives.GenericZero) flow


    /// <summary>Applies a key-generating function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumBy projection (flow : CloudFlow< ^T >) : Cloud< ^S > 
            when ^S : (static member ( + ) : ^S * ^S -> ^S) 
            and  ^S : (static member Zero : ^S) = 
        fold (fun s x -> s + projection x) (+) (fun () -> LanguagePrimitives.GenericZero) flow

    /// <summary>Applies a key-generating locally executing cloud function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    let inline sumByLocal (projection : ^T -> Local< ^Key >) (flow : CloudFlow< ^T >) : Cloud< ^Key > 
            when ^Key : (static member ( + ) : ^Key * ^Key -> ^Key) 
            and  ^Key : (static member Zero : ^Key) = 
        foldGen (fun ctx s x -> s + run ctx (projection x)) (fun _ctx x y -> x + y) (fun _ctx -> LanguagePrimitives.GenericZero) flow

    /// <summary>Returns the total number of elements of the CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The total number of elements.</returns>
    let inline length (flow : CloudFlow<'T>) : Cloud<int64> =
        fold (fun acc _  -> 1L + acc) (+) (fun () -> 0L) flow

    /// <summary>Creates an array from the given CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result array.</returns>    
    let inline toArray (flow : CloudFlow<'T>) : Cloud<'T[]> =
        cloud {
            let! arrayCollector = 
                fold (fun (acc : ArrayCollector<'T>) value -> acc.Add(value); acc)
                    (fun left right -> left.AddRange(right); left) 
                    (fun () -> new ArrayCollector<'T>()) flow 
            return arrayCollector.ToArray()
        }

    /// <summary>Creates a CloudVector from the given CloudFlow.</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudVector.</returns>    
    let toCloudVector (flow : CloudFlow<'T>) : Cloud<CloudVector<'T>> =
        cloud {
            let collectorf (cloudCts : ICloudCancellationTokenSource) = local { 
                let results = new List<List<'T>>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return 
                  { new Collector<'T, 'T []> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                    member self.Iterator() = 
                        let list = new List<'T>()
                        results.Add(list)
                        {   Index = ref -1; 
                            Func = (fun value -> list.Add(value));
                            Cts = cts }
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
            let! cts = Cloud.CreateCancellationTokenSource()
            let! vc =
                flow.Apply (collectorf cts)
                    (fun array -> local { return! CloudVector.New(array, CloudFlow.maxCloudVectorPartitionSize, enableCaching = false) }) 
                    (fun result -> local { return CloudVector.Merge result })
            return vc
        }

    /// <summary>Creates a CloudVector from the given CloudFlow, with its partitions cached locally</summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudVector.</returns>
    let toCachedCloudVector (flow : CloudFlow<'T>) : Cloud<CloudVector<'T>> =
        cloud {
            let collectorf (cloudCts : ICloudCancellationTokenSource) = local { 
                let results = new List<List<'T>>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return 
                  { new Collector<'T, 'T []> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                    member self.Iterator() = 
                        let list = new List<'T>()
                        results.Add(list)
                        {   Index = ref -1; 
                            Func = (fun value -> list.Add(value));
                            Cts = cts }
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
            let! cts = Cloud.CreateCancellationTokenSource()
            let! vc =
                flow.Apply (collectorf cts)
                    (fun array -> local { 
                                    let! cloudVector = CloudVector.New(array, CloudFlow.maxCloudVectorPartitionSize, enableCaching = true)
                                    // Cache the partitions
                                    let partitions = cloudVector.GetAllPartitions()
                                    for partition in partitions do
                                        do! partition.ForceCache() |> Local.Ignore
                                    
                                    return cloudVector
                                  }) 
                    (fun result -> local { return CloudVector.Merge result })
            return vc
        }



    let inline private sortByGen comparer (projection : ExecutionContext -> 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {  
            let results = new List<List<'T>>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return 
              { new Collector<'T, List<'Key[] * 'T []>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                member self.Iterator() = 
                    let list = new List<'T>()
                    results.Add(list)
                    {   Index = ref -1; 
                        Func = (fun value -> list.Add(value));
                        Cts = cts }
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
                let! cts = Cloud.CreateCancellationTokenSource()
                let! results = flow.Apply (collectorf cts) (fun x -> local { return x }) (fun result -> local { match result with [||] -> return List() | _ -> return Array.reduce (fun left right -> left.AddRange(right); left) result })
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
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) combiner = 
                cloud {
                    let! result = sortByComp
                    return! (CloudFlow.ofArray result).Apply collectorf projection combiner
                }  
        }

    let inline private descComparer (comparer: IComparer<'T>) = { new IComparer<'T> with member __.Compare(x,y) = -comparer.Compare(x,y) }

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortBy (projection : 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered using the given comparer for the keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsing (projection : 'T -> 'Key) comparer (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered descending by keys.</summary>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescending (projection : 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun _ctx x -> projection x) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByLocal (projection : 'T -> Local<'Key>) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = _PrivateFastGenericComparerTable<'Key>.ValueCanBeNullIfDefaultSemantics
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByUsingLocal (projection : 'T -> Local<'Key>) comparer (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by descending keys.</summary>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>  
    let inline sortByDescendingLocal (projection : 'T -> Local<'Key>) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> = 
        let comparer = descComparer LanguagePrimitives.FastGenericComparer<'Key>
        sortByGen comparer (fun ctx x -> projection x |> run ctx) takeCount flow 

    let inline private tryFindGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) =
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism 
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> if predicate ctx value then resultRef := Some value; cloudCts.Cancel() else ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return! flow.Apply (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFind (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun _ctx x -> predicate x) flow 

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Returns None if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    let inline tryFindLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<'T option> =
        tryFindGen (fun ctx x -> predicate x |> run ctx) flow 

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    let inline find (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFind predicate flow 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    let inline findLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<'T> = 
        cloud {
            let! result = tryFindLocal predicate flow 
            return
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    let inline private tryPickGen (chooser : ExecutionContext -> 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        
        let collectorf (cloudCts : ICloudCancellationTokenSource) = 
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'R option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return 
                    { new Collector<'T, 'R option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() = 
                            {   Index = ref -1; 
                                Func = (fun value -> match chooser ctx value with Some value' -> resultRef := Some value'; cloudCts.Cancel() | None -> ());
                                Cts = cts }
                        member self.Result = 
                            !resultRef }
            }
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            return! flow.Apply (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }


    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPick (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun _ctx x -> chooser x) flow 

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    let inline tryPickLocal (chooser : 'T -> Local<'R option>) (flow : CloudFlow<'T>) : Cloud<'R option> = 
        tryPickGen (fun ctx x -> chooser x |> run ctx) flow 

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    let inline pick (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPick chooser flow 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    let inline pickLocal (chooser : 'T -> Local<'R option>) (flow : CloudFlow<'T>) : Cloud<'R> = 
        cloud {
            let! result = tryPickLocal chooser flow 
            return 
                match result with
                | Some value -> value 
                | None -> raise <| new KeyNotFoundException()
        }

    /// <summary>Tests if any element of the flow satisfies the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline exists (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFind predicate flow 
            return 
                match result with
                | Some value -> true
                | None -> false
        }

    /// <summary>Tests if any element of the flow satisfies the given locally executing cloud predicate.</summary>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    let inline existsLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = tryFindLocal predicate flow 
            return 
                match result with
                | Some value -> true
                | None -> false
        }


    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forall (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = exists (fun x -> not <| predicate x) flow
            return not result
        }


    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <param name="flow">The input cloud flow.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    let inline forallLocal (predicate : 'T -> Local<bool>) (flow : CloudFlow<'T>) : Cloud<bool> = 
        cloud {
            let! result = existsLocal (fun x -> local { let! v = predicate x in return not v }) flow
            return not result
        }

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="n">The maximum number of items to take.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The resulting CloudFlow.</returns>
    let inline take (n : int) (flow: CloudFlow<'T>) : CloudFlow<'T> =
        let collectorF (cloudCts : ICloudCancellationTokenSource) =
            local {
                let results = new List<List<'T>>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T []> with
                      member __.DegreeOfParallelism = flow.DegreeOfParallelism
                      member __.Iterator() =
                          let list = new List<'T>()
                          results.Add(list)
                          { Index = ref -1
                            Func = (fun value -> if list.Count < n then list.Add(value) else cloudCts.Cancel())
                            Cts = cts }
                      member __.Result =
                          (results |> Seq.concat).Take(n) |> Seq.toArray
                     }
            }
        let gather =
            cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let! results = flow.Apply (collectorF cts) (local.Return) (fun results -> local { return Array.concat results })
                return results.Take(n).ToArray()
            }
        { new CloudFlow<'T> with
              member __.DegreeOfParallelism = flow.DegreeOfParallelism
              member __.Apply<'S, 'R>(collectorF: Local<Collector<'T, 'S>>) (projection: 'S -> Local<'R>) combiner =
                  cloud {
                      let! result = gather
                      return! (CloudFlow.ofArray result).Apply collectorF projection combiner
                  }
        }

    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using text line reader.
    /// </summary>
    /// <param name="sources">The collection of CloudFiles.</param>
    [<CompilerMessage("This is producer, not consumer; missing optional encoding param.", 523)>]
    let ofCloudFilesByLine (sources : seq<string>) : CloudFlow<string> =
        sources
        |> CloudFlow.ofCloudFiles CloudFileReader.ReadLines
        |> collect id



    /// <summary>Sends the values of CloudFlow to the SendPort of a CloudChannel</summary>
    /// <param name="channel">the SendPort of a CloudChannel.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    let toCloudChannel (channel : ISendPort<'T>) (flow : CloudFlow<'T>)  : Cloud<unit> =
        flow |> iterLocal (fun v -> CloudChannel.Send(channel, v))
