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
type CloudFlow =

    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="source">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    static member OfArray (source : 'T []) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    // local worker ParStream workflow
                    let createTask array = local {
                        let! collector = collectorf
                        let parStream = ParStream.ofArray array 
                        let collectorResult = parStream.Apply (collector.ToParStreamCollector())
                        return! projection collectorResult
                    }

                    let! collector = collectorf
                    let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                    let! workers = Cloud.GetAvailableWorkers() 
                    // force deterministic scheduling sorting by worker id
                    let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                    let workers =
                        match collector.DegreeOfParallelism with
                        | None -> workers
                        | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]
                    
                    let! results =
                        if targetedworkerSupport then
                            source
                            |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                            |> Seq.filter (not << Array.isEmpty << snd)
                            |> Seq.map (fun (w,partition) -> createTask partition, w)
                            |> Cloud.Parallel
                        else
                            source
                            |> WorkerRef.partition workers
                            |> Seq.filter (not << Array.isEmpty << snd)
                            |> Seq.map (createTask << snd)
                            |> Cloud.Parallel

                    return! combiner results 
                } }

    /// <summary>
    ///     Creates a CloudFlow according to partitions of provided cloud collection.
    /// </summary>
    /// <param name="collection">Input cloud collection.</param>
    /// <param name="useCache">Make use of caching, if the collection supports it. Defaults to false.</param>
    /// <param name="sizePerCoreThreshold">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfCloudCollection (collection : ICloudCollection<'T>, ?useCache:bool, ?sizePerCoreThreshold:int64) : CloudFlow<'T> =
        do sizePerCoreThreshold |> Option.iter (function t when t <= 0L -> invalidArg "maxBytesPerWorker" "must be positive." | _ -> ())
        let useCache = defaultArg useCache false
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    // TODO: take nested partitioning into account
                    let! collector = collectorf
                    let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                    let! workers = Cloud.GetAvailableWorkers()
                    // sort by id to ensure deterministic scheduling
                    let workers = workers |> Array.sortBy (fun w -> w.Id)
                    let workers =
                        match collector.DegreeOfParallelism with
                        | None -> workers
                        | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]

                    // detect partitions for collection, if any
                    let! partitions = local {
                        match collection with
                        | :? IPartitionedCollection<'T> as pcc -> let! partitions = pcc.GetPartitions() in return Some partitions
                        | :? IPartitionableCollection<'T> as pcc ->
                            // got a collection that can be partitioned dynamically;
                            // partition according core counts per worker
                            let partitionCount = workers |> Array.map (fun w -> w.ProcessorCount) |> Array.gcdNormalize |> Array.sum
                            let! partitions = pcc.GetPartitions partitionCount 
                            return Some partitions

                        | _ -> return None
                    }

                    // use caching, if supported by collection
                    let tryGetCachedContents (collection : ICloudCollection<'T>) = local {
                        match box collection with
                        | :? Store.ICloudCacheable<'T []> as cc -> 
                            if useCache then 
                                let! result = CloudCache.GetCachedValue(cc, cacheIfNotExists = true)
                                return Some result
                            else 
                                return! CloudCache.TryGetCachedValue cc

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

                        let arrayFlow = CloudFlow.OfArray array
                        return! arrayFlow.Apply collectorf projection combiner

                    | Some partitions ->
                        // have partitions, schedule according to number of partitions.
                        let createTask (partitions : ICloudCollection<'T> []) = local {
                            // further partition according to collection size threshold, if so specified.
                            let! partitionss =
                                match sizePerCoreThreshold with
                                | None -> local { return [|partitions|] }
                                | Some sz -> partitions |> partitionBySize (fun p -> p.Size) (int64 Environment.ProcessorCount * sz)

                            // compute a single partition
                            let computePartition (partition : ICloudCollection<'T> []) = local {
                                let getSeq (p : ICloudCollection<'T>) = local {
                                    let! cached = tryGetCachedContents p
                                    match cached with
                                    | Some c -> return c :> seq<'T>
                                    | None -> return! p.ToEnumerable()
                                }

                                let! collector = collectorf
                                let! seqs = Sequential.map getSeq partitions
                                let pStream = seqs |> ParStream.ofArray |> ParStream.collect Stream.ofSeq
                                let value = pStream.Apply (collector.ToParStreamCollector())
                                return! projection value
                            }

                            // sequentially compute partitions
                            let! results = Sequential.map computePartition partitionss
                            return! combiner results
                        }
                        
                        let! results =
                            if targetedworkerSupport then
                                partitions
                                |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (fun (w,partitions) -> createTask partitions, w)
                                |> Cloud.Parallel
                            else
                                partitions
                                |> WorkerRef.partition workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (createTask << snd)
                                |> Cloud.Parallel

                        return! combiner results
                }
        }

    /// <summary>
    ///     Creates a CloudFlow instance from a finite collection of serializable enumerations.
    /// </summary>
    /// <param name="enumerations">Input enumerations.</param>
    static member OfSeqs (enumerations : seq<#seq<'T>>) : CloudFlow<'T> =
        let enumerations = Seq.toArray enumerations
        let getCount (seq : seq<'T>) = 
            match seq with
            | :? ('T list) as ts -> ts.Length
            | :? ICollection<'T> as c -> c.Count
            | _ -> Seq.length seq
            |> int64

        let mkSeqCollection (seq : seq<'T>) =
            {
                new ICloudCollection<'T> with
                    member x.Count: Local<int64> = local { return getCount seq }
                    member x.Size: Local<int64> = local { return getCount seq }
                    member x.ToEnumerable(): Local<seq<'T>> = local { return seq }
            }

        let partitioned =
            {
                new IPartitionedCollection<'T> with
                    member x.Count: Local<int64> = local { return enumerations |> Array.sumBy getCount }
                    member x.Size: Local<int64> = local { return enumerations |> Array.sumBy getCount }
                    member x.GetPartitions(): Local<ICloudCollection<'T> []> = local { return enumerations |> Array.map mkSeqCollection }
                    member x.PartitionCount: Local<int> = local { return enumerations.Length }
                    member x.ToEnumerable(): Local<seq<'T>> = local { return Seq.concat enumerations }
            }

        CloudFlow.OfCloudCollection partitioned


    /// <summary>
    /// Constructs a CloudFlow from a collection of CloudFiles using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizePerCoreThreshold">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfFiles (reader : System.IO.Stream -> seq<'T>, sources : seq<string>, ?enableCache : bool, ?sizePerCoreThreshold : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let toCloudSeq (path : string) = CloudSequence.FromFile(path, reader, ?enableCache = enableCache)
                    let! cseqs = Sequential.map toCloudSeq sources
                    let collection =
                        { new IPartitionedCollection<'T> with
                            member x.Count: Local<int64> = local { let! counts = cseqs |> Sequential.map (fun c -> c.Count) in return Array.sum counts }
                            member x.Size: Local<int64> = local { let! sizes = cseqs |> Sequential.map (fun c -> c.Size) in return Array.sum sizes }
                            member x.GetPartitions(): Local<ICloudCollection<'T> []> = local { return cseqs |> Array.map (fun c -> c :> ICloudCollection<'T>) }
                            member x.PartitionCount: Local<int> = local { return cseqs.Length }
                            member x.ToEnumerable(): Local<seq<'T>> = local { let! enums = cseqs |> Sequential.map (fun c -> c.ToEnumerable()) in return Seq.concat enums } 
                        }

                    let collectionFlow = CloudFlow.OfCloudCollection(collection, ?useCache = enableCache, ?sizePerCoreThreshold = sizePerCoreThreshold)
                    return! collectionFlow.Apply collectorf projection combiner
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow from a collection of text files using the given reader.
    /// </summary>
    /// <param name="reader">A function to transform the contents of a CloudFile to a stream of elements.</param>
    /// <param name="sources">The collection of CloudFiles.</param>
    /// <param name="enableCache">Enable use of caching for deserialized values. Defaults to false.</param>
    /// <param name="sizePerCoreThreshold">Restricts concurrent processing of collection partitions up to specified size per core. Defaults to 256MiB.</param>
    static member OfTextFiles (reader : System.IO.TextReader -> seq<'T>, sources : seq<string>, ?encoding : Encoding, ?enableCache : bool, ?sizePerCoreThreshold : int64) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let reader (stream : System.IO.Stream) =
                        let sr =
                            match encoding with
                            | None -> new System.IO.StreamReader(stream)
                            | Some e -> new System.IO.StreamReader(stream, e)

                        reader sr

                    let filesFlow = CloudFlow.OfFiles(reader, sources, ?enableCache = enableCache, ?sizePerCoreThreshold = sizePerCoreThreshold)
                    return! filesFlow.Apply collectorf projection combiner
                }
        }

    /// <summary>
    /// Constructs a CloudFlow from a CloudVector.
    /// </summary>
    /// <param name="source">The input CloudVector.</param>
    static member OfCloudVector (source : CloudVector<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let useCache = source.IsCachingSupported
                    let partitions = [| 0 .. source.PartitionCount - 1 |]

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
                        return! combiner [||]
                    else
                        let! collector = collectorf
                        let! targetedworkerSupport = Cloud.IsTargetedWorkerSupported
                        let! workers = Cloud.GetAvailableWorkers() 
                        // force deterministic scheduling sorting by worker id
                        let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                        let workers =
                            match collector.DegreeOfParallelism with
                            | None -> workers
                            | Some dp -> [| for i in 0 .. dp - 1 -> workers.[i % workers.Length] |]

                        let! results =
                            if targetedworkerSupport then
                                partitions
                                |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (fun (w,partitions) -> computePartitions partitions, w)
                                |> Cloud.Parallel
                            else
                                partitions
                                |> WorkerRef.partition workers
                                |> Seq.filter (not << Array.isEmpty << snd)
                                |> Seq.map (computePartitions << snd)
                                |> Cloud.Parallel

                        return! combiner results
                }
        }

    /// <summary>
    ///     Constructs a CloudFlow of lines from a single large text file.
    /// </summary>
    /// <param name="path">The path to the text file.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfTextFileByLine (path : string, ?encoding : Encoding) : CloudFlow<string> = 
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let! cseq = CloudSequence.FromLineSeparatedTextFile(path, ?encoding = encoding, enableCache = false, force = false)
                let collectionStream = CloudFlow.OfCloudCollection cseq
                return! collectionStream.Apply collectorf projection combiner
            }  
        }

    /// <summary>
    ///     Constructs a CloudFlow of lines from a collection of text files.
    /// </summary>
    /// <param name="paths">Paths to the text files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfTextFilesByLine (paths : seq<string>, ?encoding : Encoding) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let flow = CloudFlow.OfFiles ((fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding)), paths)
                return! flow.Apply collectorf projection combiner
            }
        }

    /// <summary>
    ///     Constructs a CloudFlow of text bodies from a collection of text files.
    /// </summary>
    /// <param name="paths">Paths to the text files.</param>
    /// <param name="encoding">Optional encoding.</param>
    static member OfTextFiles (paths : seq<string>, ?encoding : Encoding) : CloudFlow<string> =
        { new CloudFlow<string> with
            member self.DegreeOfParallelism = None
            member self.Apply<'S, 'R> (collectorf : Local<Collector<string, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) = cloud {
                let flow = CloudFlow.OfFiles ((fun stream -> Seq.singleton <| TextReaders.ReadAllText(stream, ?encoding = encoding)), paths)
                return! flow.Apply collectorf projection combiner
            }
        }
                
    /// <summary>Creates a CloudFlow from the ReceivePort of a CloudChannel</summary>
    /// <param name="channel">the ReceivePort of a CloudChannel.</param>
    /// <param name="degreeOfParallelism">The number of concurrently receiving tasks</param>
    /// <returns>The result CloudFlow.</returns>
    static member OfCloudChannel (channel : IReceivePort<'T>, degreeOfParallelism : int) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = Some degreeOfParallelism
            member self.Apply<'S, 'R> (collectorf : Local<Collector<'T, 'S>>) (projection : 'S -> Local<'R>) (combiner : 'R [] -> Local<'R>) =
                cloud {
                    let! collector = collectorf 
                    let! workers = Cloud.GetAvailableWorkers() 
                    let workers = workers |> Array.sortBy (fun workerRef -> workerRef.Id)
                    let workerCount = defaultArg collector.DegreeOfParallelism workers.Length

                    let createTask () = local {
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
                            Seq.init workerCount (fun i -> (createTask (), workers.[i % workers.Length])) 
                            |> Cloud.Parallel
                        else
                            Seq.init workerCount (fun _ -> createTask ())
                            |> Cloud.Parallel

                    return! combiner results

                } }