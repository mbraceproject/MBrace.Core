namespace MBrace.Streams

open MBrace

open MBrace

open System
open System.Collections
open System.Collections.Generic
open MBrace.Store
open MBrace
open MBrace.Workflows
open MBrace.Continuation

#nowarn "444"

type CacheMap = IDictionary<IWorkerRef, ICloudStorageEntity []> option

type CloudVector<'T> (count : int64, partitions : CloudSequence<'T> [], cacheMap) = 
    interface ICloudDisposable with
        member this.Dispose(): Cloud<unit> = 
            cloud {
                do! partitions
                    |> Seq.map Cloud.Dispose
                    |> Cloud.Parallel
                    |> Cloud.Ignore
                do! cacheMap.Dispose()
            }
         
    member val Count = count
    member val Partitions = partitions
    member val PartitionCount = partitions.Length

    member val CacheMap : ICloudAtom<CacheMap> = cacheMap

    member this.ToEnumerable () : Cloud<IEnumerable<'T>> =
        cloud {
            // TODO : Replace with Sequential.lazyCollector
            let! ctx = Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx)
            return seq {
                for t in this.Partitions do yield! Cloud.RunSynchronously(t.ToEnumerable(), ctx.Resources, ctx.CancellationToken)
            }
        }


type Cache = 
    static member CreateMap(vector : CloudVector<'T>, workers : IWorkerRef seq) : CacheMap = 
        let workers = workers |> Seq.sort |> Seq.toArray
        let workerCount = workers.Length
        
        let map = 
            vector.Partitions
            |> Seq.mapi (fun i p -> i, p :> ICloudStorageEntity)
            |> Seq.groupBy (fun (i, _) -> i % workerCount)
            |> Seq.map (fun (key, values) -> 
                    workers.[key], 
                    values
                    |> Seq.map snd
                    |> Seq.toArray)
            |> Map.ofSeq
        Some(map :> _)

    static member Combine(state1 : CacheMap, state2 : CacheMap) : CacheMap =
        None

type VectorCollector<'T> (count, partitions, cacheMap) =
    member val Count = count
    member val Partitions = partitions
    member val CacheMap = cacheMap

    member __.ToCloudVector () = 
        cloud {
            let! map = CloudAtom.New(cacheMap)
            return new CloudVector<'T>(count, partitions, map)
        }

    member __.ToEnumerable () : Cloud<IEnumerable<'T>> =
        cloud {
            // TODO : Replace with Sequential.lazyCollector
            let! ctx = Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx)
            return seq {
                for t in partitions do yield! Cloud.RunSynchronously(t.ToEnumerable(), ctx.Resources, ctx.CancellationToken)
            }
        }

    member v1.Merge(v2 : VectorCollector<'T>) =
        let count = v1.Count + v2.Count
        let partitions = Array.append v1.Partitions v2.Partitions
        let cache = Cache.Combine(v1.CacheMap, v2.CacheMap)
        new VectorCollector<'T>(count, partitions, cache)

    static member New(values : seq<'T>, maxPartitionSize : int64) =
        cloud {
            let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize)
            let count = ref 0L
            for p in partitions do 
                let! c = p.Count
                count := !count + int64 c
            let! context = Cloud.GetSchedulingContext()
            let! map  = 
                match context with
                | ThreadParallel
                | Sequential -> cloud.Return None
                | Distributed -> cloud { 
                    let! w = Cloud.CurrentWorker
                    let ps = partitions |> Array.map (fun p -> p :> ICloudStorageEntity) 
                    return (w, ps)
                            |> Seq.singleton
                            |> Map.ofSeq :> IDictionary<_, _>
                            |> Some 
                }
            return new VectorCollector<'T>(count.Value, partitions, map)
        }

type CloudVector =
    static member Merge(v1 : CloudVector<'T>, v2 : CloudVector<'T>) : Cloud<CloudVector<'T>> = 
        cloud {
            let count = v1.Count + v2.Count
            let partitions = Array.append v1.Partitions v2.Partitions
            let! map1, map2 = v1.CacheMap.Value <||> v2.CacheMap.Value
            let! map = CloudAtom.New(Cache.Combine(map1, map2))
            return new CloudVector<'T>(count, partitions, map) 
        }

    static member New<'T>(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer) : Cloud<CloudVector<'T>> =
        cloud {
            let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize, ?directory = directory, ?serializer = serializer)
            let count = ref 0L
            for p in partitions do 
                let! c = p.Count
                count := !count + int64 c
            let! map = CloudAtom.New(None)
            return new CloudVector<'T>(count.Value, partitions, map) 
        }

    static member NoCache(vector : CloudVector<'T>) = 
        cloud {
            return! vector.CacheMap.Force(None)
        }

    static member Cache(vector : CloudVector<'T>, workers : IWorkerRef seq) : Cloud<CacheMap> =
        cloud {
            let cacheMap = Cache.CreateMap(vector, workers)
            do! vector.CacheMap.Force(cacheMap)
            return cacheMap
        }

    static member Cache(vector : CloudVector<'T>) : Cloud<unit> =
        cloud {
            let! context = Cloud.GetSchedulingContext()
            let! isTargetSupported = Cloud.IsTargetedWorkerSupported
            match context with
            | Sequential | ThreadParallel -> 
                return failwith "Cannot Cache in context %A. Only %A context is supported." context Distributed
            | Distributed when not isTargetSupported ->
                return failwith "Cannot Cache in runtimes not supporting worker targeting."
            | Distributed ->
                let! workers = Cloud.GetAvailableWorkers()
                let! _ = CloudVector.Cache(vector, workers)
                return ()
        }


//[<AutoOpen>]
//module StoreClientExtensions =
//    open System.Runtime.CompilerServices
//    
//    /// Common operations on CloudVectors.
//    type CloudVectorClient internal (resources : ResourceRegistry) =
//        let toAsync wf = Cloud.ToAsync(wf, resources)
//        let toSync wf = Cloud.RunSynchronously(wf, resources)
//
//        /// <summary>
//        /// Create a new cloud array.
//        /// </summary>
//        /// <param name="values">Collection to populate the cloud array with.</param>
//        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
//        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
//        /// <param name="partitionSize">Approximate partition size in bytes.</param>
//        member __.NewAsync(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) =
//            CloudVector.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
//            |> toAsync
//    
//        /// <summary>
//        /// Create a new cloud array.
//        /// </summary>
//        /// <param name="values">Collection to populate the cloud array with.</param>
//        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
//        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
//        /// <param name="partitionSize">Approximate partition size in bytes.</param>
//        member __.New(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) =
//            CloudVector.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
//            |> toSync
//    
//    [<Extension>]
//    type MBrace.Client.StoreClient with
//        [<Extension>]
//        /// CloudVector client.
//        member this.CloudVector = new CloudVectorClient(this.Resources)