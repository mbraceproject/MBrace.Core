namespace MBrace.Flow

open System
open System.Threading
open System.Text
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace
open MBrace.Store
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Flow
open MBrace.Flow.Internals

#nowarn "444"

/// Provides CloudFlow producers.
type CloudFlow private () =
    /// Maximum combined flow length used in ofCloudFiles.
    static let maxCloudFileCombinedLength = 1024L * 1024L * 1024L

    /// gets all partition indices found in cloud vector
    static let getPartitionIndices (v : CloudVector<'T>) = [| 0 .. v.PartitionCount - 1 |]

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
                            let collectorResult = parStream.Apply (collector.ToParStreamCollector())
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

                            let collectorResult = parStream.Apply (collector.ToParStreamCollector())
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
    ///     Creates a CloudFlow instance from a finite collection of serializable enumerations.
    /// </summary>
    /// <param name="enumerations">Input enumerations.</param>
    static member ofSeqs (enumerations : seq<#seq<'T>>) : CloudFlow<'T> =
        let enumerations = Seq.toArray enumerations
        let mkSeqCollection (seq : seq<'T>) = 
            {
                new ICloudCollection<'T> with
                    member x.Count: Local<int64> = local { return int64 <| Seq.length seq }
                    member x.Size: Local<int64> = local { return int64 <| Seq.length seq }
                    member x.ToEnumerable(): Local<seq<'T>> = local { return seq }
            }

        let partitioned =
            {
                new IPartitionedCollection<'T> with
                    member x.Count: Local<int64> = local { return enumerations |> Array.map (int64 << Seq.length) |> Array.sum }
                    member x.Size: Local<int64> = local { return enumerations |> Array.map (int64 << Seq.length) |> Array.sum }
                    member x.GetPartitions(): Local<ICloudCollection<'T> []> = local { return enumerations |> Array.map mkSeqCollection }
                    member x.PartitionCount: Local<int> = local { return enumerations.Length }
                    member x.ToEnumerable(): Local<seq<'T>> = local { return Seq.concat enumerations }
            }

        CloudFlow.ofCloudCollection partitioned


    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    static member ofCloudFiles (reader : System.IO.Stream -> seq<'T>) (sources : seq<string>) : CloudFlow<'T> =
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
                                // sequentially process partitions grouped by accumulated size
                                for fs in partitions do
                                    let! collector = collectorf
                                    let parStream = 
                                        fs
                                        |> ParStream.ofSeq 
                                        |> ParStream.map (fun file -> CloudFile.Read(file.Path, (fun s -> async { return reader s }), leaveOpen = true))
                                        |> ParStream.map (fun wf -> Cloud.RunSynchronously(wf, ctx.Resources, ctx.CancellationToken))
                                        |> ParStream.collect Stream.ofSeq

                                    let collectorResult = parStream.Apply (collector.ToParStreamCollector())
                                    let! partial = projection collectorResult
                                    result.Add partial

                                return! combiner (result.ToArray())
                            }

                        let files = sources |> Seq.map (fun p -> new CloudFile(p)) |> Seq.toArray
                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! results = 
                            if targetedworkerSupport then
                                files
                                |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                                |> Array.mapi (fun i (worker,cfiles) -> createTask cfiles collectorf, worker) 
                                |> Cloud.Parallel
                            else
                                files
                                |> Array.splitByPartitionCount workers.Length 
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
                    let partitions = getPartitionIndices source

                    let computePartitions (partitions : int []) = local {

                        let computePartition (pIndex : int) = local {
                            let partition = source.GetPartition pIndex
                            let! collector = collectorf
                            if useCache then do! partition.ForceCache() |> Local.Ignore
                            let! array = partition.ToArray()
                            let parStream = ParStream.ofArray array 
                            let collectorResult = parStream.Apply (collector.ToParStreamCollector())
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

    /// <summary>
    ///     Constructs a CloudFlow of lines from a collection of text files.
    /// </summary>
    /// <param name="paths">Paths to the text files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member ofTextFilesByLine (paths : seq<string>, ?encoding : Encoding) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let flow = CloudFlow.ofCloudFiles (fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding)) paths
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a CloudFlow of text bodies from a collection of text files.
    /// </summary>
    /// <param name="paths">Paths to the text files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member ofTextFiles (paths : seq<string>, ?encoding : Encoding) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let flow = CloudFlow.ofCloudFiles (fun stream -> Seq.singleton <| TextReaders.ReadAllText(stream, ?encoding = encoding)) paths
                return! flow.Apply collectorf projection combiner
            }
        }
                
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

                    let createTask (collectorf : Local<Collector<'T, 'S>>) = 
                        local {
                            let! ctx = Cloud.GetExecutionContext()
                            let! collector = collectorf
                            let seq = Seq.initInfinite (fun _ -> Cloud.RunSynchronously(CloudChannel.Receive channel, ctx.Resources, ctx.CancellationToken))
                            let parStream = ParStream.ofSeq seq
                            let collectorResult = parStream.Apply (collector.ToParStreamCollector())
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