namespace MBrace.Streams

open System
open System.Collections
open System.Collections.Generic
open MBrace.Store
open MBrace

type internal PartitionId = string
type internal CacheMap = IDictionary<IWorkerRef, PartitionId []>

type CloudVector<'T> internal (count : int64, partitions, cacheMap) =
    member val Count : int64 = count with get
    
    member val Partitions : CloudSequence<'T> [] = partitions with get
    
    member this.PartitionCount with get () : int = this.Partitions.Length
    
    member val internal CacheMap : ICloudAtom<CacheMap option> = cacheMap 

type CloudVector =
    static member Merge(v1 : CloudVector<'T>, v2 : CloudVector<'T>) = 
        cloud {
            let count = v1.Count + v2.Count
            let partitions = Array.append v1.Partitions v2.Partitions
            let! map = CloudAtom.New(None)
            return new CloudVector<'T>(count, partitions, map)
        }

    static member Merge(vs : CloudVector<'T> seq) =
        cloud {
            let count = vs |> Seq.sumBy (fun v -> v.Count)
            let partitions = 
                vs |> Seq.map (fun v -> v.Partitions)
                   |> Array.concat
            let! map = CloudAtom.New(None)
            return new CloudVector<'T>(count, partitions, map)
        }

    static member New<'T>(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer) =
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

    static member internal Cache(vector : CloudVector<'T>, workers : IWorkerRef seq) : Cloud<CacheMap> =
        cloud {
            let cacheMap = Cache.CreateMap(vector, workers)
            do! vector.CacheMap.Force(Some cacheMap)
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

and internal Cache = 
    static member CreateMap(vector : CloudVector<'T>, workers : IWorkerRef seq) : CacheMap = 
        let workers = Seq.toArray workers
        let workerCount = workers.Length
        
        let map = 
            vector.Partitions
            |> Seq.mapi (fun i p -> i, (p :> ICloudStorageEntity).Id)
            |> Seq.groupBy (fun (i, _) -> i % workerCount)
            |> Seq.map (fun (key, values) -> 
                   workers.[key], 
                   values
                   |> Seq.map snd
                   |> Seq.toArray)
            |> Map.ofSeq
        map :> _

    static member Combine(states : CacheMap option seq) : CacheMap option =
        None

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