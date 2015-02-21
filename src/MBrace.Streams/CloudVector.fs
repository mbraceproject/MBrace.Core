namespace MBrace.Streams

open System.Collections.Generic

open MBrace
open MBrace.Store
open MBrace.Workflows

#nowarn "444"

///// Worker to cached index mapping
//type CacheState = IDictionary<IWorkerRef, int []>

type WorkerCacheState = IWorkerRef * int []

/// Represents an ordered collection of values stored in CloudSequence partitions.
[<AbstractClass>]
type CloudVector<'T> () =
    /// Gets the total element count for the cloud vector.
    abstract Count : Local<int64>
    /// Gets the partition count for cloud vector.
    abstract PartitionCount : int
    /// Gets all partitions of contained in the vector.
    abstract GetAllPartitions : unit -> CloudSequence<'T> []
    /// Gets partition of given index.
    abstract GetPartition : index:int -> CloudSequence<'T>
    /// Gets partition of given index.
    abstract Item : int -> CloudSequence<'T> with get
    /// Returns a local enumerable that iterates through
    /// all elements of the cloud vector.
    abstract ToEnumerable : unit -> Local<seq<'T>>
    /// Gets the cache support status for cloud vector instance.
    abstract IsCachingSupported : bool
    /// Gets the current cache state of the vector inside the cluster.
    abstract GetCacheState : unit -> Local<WorkerCacheState []>
    /// Updates the cache state to include provided indices for given worker ref.
    abstract UpdateCacheState : worker:IWorkerRef * appendedIndices:int[] -> Local<unit>

    abstract Dispose : unit -> Local<unit>

    interface ICloudDisposable with
        member __.Dispose () = __.Dispose()

type internal AtomCloudVector<'T>(elementCount : int64 option, partitions : CloudSequence<'T> [], cacheMap : ICloudAtom<Map<IWorkerRef, int[]>> option) =
    inherit CloudVector<'T> ()

    let mutable elementCount = elementCount

    let getCacheMap() =
        match cacheMap with
        | None -> raise <| new System.NotSupportedException("caching")
        | Some cm -> cm
        

    override __.Count = local {
        match elementCount with
        | Some c -> return c
        | None ->
            let! counts =
                partitions
                |> Seq.map (fun p -> p.Count)
                |> Cloud.LocalParallel

            let count = counts |> Array.sumBy int64
            elementCount <- Some count
            return count
    }

    override __.PartitionCount = partitions.Length
    override __.GetAllPartitions () = partitions
    override __.GetPartition i = partitions.[i]
    override __.Item with get i = partitions.[i]
    override __.ToEnumerable() = local {
        return! partitions |> Sequential.lazyCollect (fun p -> p.ToEnumerable())
    }

    override __.IsCachingSupported = Option.isSome cacheMap
    override __.GetCacheState () = getCacheMap().Value |> Cloud.map (fun m -> m |> Seq.map (function KeyValue(w,is) -> (w,is)) |> Seq.toArray)

    override __.UpdateCacheState(worker : IWorkerRef, appendedIndices : int []) = local {
        let cacheMap = getCacheMap()
        let updater (state : Map<IWorkerRef, int[]>) =
            let indices =
                match state.TryFind worker with
                | None -> appendedIndices
                | Some is -> Seq.append is appendedIndices |> Seq.distinct |> Seq.toArray

            state.Add(worker, indices)

        return! cacheMap.Update updater
    }

    override __.Dispose() = local {
        return!
            partitions
            |> Array.map (fun p -> (p :> ICloudDisposable).Dispose())
            |> Cloud.LocalParallel
            |> Cloud.Ignore
    }

type internal ConcatenatedCloudVector<'T>(components : CloudVector<'T> []) =
    inherit CloudVector<'T> ()

    // computing global index for jagged array

    let global2Local (globalIndex : int) =
        if globalIndex < 0 then raise <| new System.IndexOutOfRangeException()

        let mutable ci = 0
        let mutable i = globalIndex
        while ci < components.Length && i >= components.[ci].PartitionCount do
            ci <- ci + 1
            i <- i - components.[ci].PartitionCount

        if ci = components.Length then raise <| new System.IndexOutOfRangeException()
        ci,i

    let local2Global (componentIndex : int, partitionIndex : int) =
        let mutable globalIndex = partitionIndex
        for ci = 0 to componentIndex - 1 do
            globalIndex <- globalIndex + components.[ci].PartitionCount

        globalIndex

    member internal __.Components = components

    override __.Count = local {
        let! counts = components |> Sequential.map (fun c -> c.Count)
        return Array.sum counts
    }

    override __.PartitionCount = components |> Array.sumBy(fun c -> c.PartitionCount)
    override __.ToEnumerable() = local {
        return! components |> Sequential.lazyCollect(fun p -> p.ToEnumerable())
    }

    override __.GetAllPartitions () = components |> Array.collect(fun c -> c.GetAllPartitions())
    override __.GetPartition i = let ci, pi = global2Local i in components.[ci].[pi]
    override __.Item with get i = let ci, pi = global2Local i in components.[ci].[pi]

    override __.IsCachingSupported = components |> Array.forall(fun c -> c.IsCachingSupported)
    override __.GetCacheState() = local {
        let getComponentCacheState (ci : int) (c : CloudVector<'T>) = local {
            let! state = c.GetCacheState()
            // transform indices before returning
            return state |> Array.map (fun (w,is) -> w, is |> Array.map (fun i -> local2Global (ci, i)))
        }
            
        let! states =
            components
            |> Seq.mapi getComponentCacheState
            |> Cloud.LocalParallel

        return
            states
            |> Seq.concat
            |> Seq.groupBy fst
            |> Seq.map (fun (w, css) -> w, css |> Seq.collect (fun (_, is) -> is) |> Seq.distinct |> Seq.toArray)
            |> Seq.toArray
    }

    override __.UpdateCacheState(w : IWorkerRef, indices : int[]) = local {
        let groupedIndices =
            indices
            |> Seq.map global2Local
            |> Seq.groupBy fst
            |> Seq.map (fun (ci, iss) -> ci, iss |> Seq.map snd |> Seq.toArray)
            |> Seq.toArray

        do!
            groupedIndices
            |> Seq.map (fun (ci, iss) -> components.[ci].UpdateCacheState(w, iss))
            |> Cloud.LocalParallel
            |> Cloud.Ignore
    }

    override __.Dispose () = local {
        do!
            components
            |> Seq.map (fun c -> c.Dispose())
            |> Cloud.LocalParallel
            |> Cloud.Ignore
    }
            

type CloudVector =

    static member OfPartitions(partitions : seq<CloudSequence<'T>>, ?enableCaching : bool) : Cloud<CloudVector<'T>> = cloud {
        let partitions = Seq.toArray partitions
        if Array.isEmpty partitions then invalidArg "partitions" "partitions must be non-empty sequence."
        let! cacheAtom = cloud {
            if defaultArg enableCaching true then 
                let! ca = CloudAtom.New Map.empty<IWorkerRef, int[]>
                return Some ca
            else return None
        }

        return new AtomCloudVector<'T>(None, partitions, cacheAtom) :> CloudVector<'T>
    }

    static member Merge(components : seq<CloudVector<'T>>) : CloudVector<'T> =
        let components = 
            components
            |> Seq.collect (function :? ConcatenatedCloudVector<'T> as c -> c.Components | v -> [|v|])
            |> Seq.toArray

        new ConcatenatedCloudVector<'T>(components) :> CloudVector<'T>


    static member New<'T>(values : seq<'T>, maxPartitionSize : int64, ?enableCaching:bool) : Cloud<CloudVector<'T>> = cloud {
        let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize)
        return! CloudVector.OfPartitions(partitions, ?enableCaching = enableCaching)
    }


//type CloudVector2<'T> internal (count : int64, partitions : CloudSequence<'T> [], cacheMap : ICloudAtom<CacheMap<'T>>) = 
//    interface ICloudDisposable with
//        member this.Dispose(): Cloud<unit> = 
//            cloud {
//                do! partitions
//                    |> Seq.map Cloud.Dispose
//                    |> Cloud.Parallel
//                    |> Cloud.Ignore
//                do! cacheMap.Dispose()
//            }
//         
//    /// Total number of values stored.
//    member val Count = count
//    /// Get CloudVector's partitions.
//    member val Partitions = partitions
//    /// Number of partitions.
//    member val PartitionCount = partitions.Length
//    /// Get current CacheMap.
//    member val CacheMap : ICloudAtom<CacheMap<'T>> = cacheMap
//
//    /// Returns an enumeration of CloudVector's values.
//    member __.ToEnumerable () : Cloud<IEnumerable<'T>> =
//        cloud {
//            return! partitions |> Sequential.lazyCollect (fun p -> p.ToEnumerable())
//        }
//
//    /// <summary>
//    /// Reset CloudVector's cache-map.
//    /// </summary>
//    /// <param name="vector">Input CloudVector.</param>
//    abstract NoCache : unit -> Cloud<unit>
//    default vector.NoCache() = 
//        cloud {
//            return! vector.CacheMap.Force(None)
//        }
//
//    /// <summary>
//    /// Calculate a cache-map for the given CloudVector, based on available runtime workers.
//    /// This method does not affect InMemory runtimes and is not supported in distributed runtimes 
//    /// without worker targeting.
//    /// </summary>
//    /// <param name="vector">Input CloudVector.</param>
//    abstract Cache : unit -> Cloud<unit>
//    default vector.Cache() : Cloud<unit> =
//        cloud {
//            let! context = Cloud.GetSchedulingContext()
//            let! isTargetSupported = Cloud.IsTargetedWorkerSupported
//            match context with
//            | Sequential | ThreadParallel -> 
//                do! vector.CacheMap.Force(None)
//                return ()
//            | Distributed when not isTargetSupported ->
//                return failwith "Cannot Cache in runtimes not supporting worker targeting."
//            | Distributed ->
//                let! workers = Cloud.GetAvailableWorkers()
//                let cacheMap = CacheState.Create(vector, workers)
//                do! vector.CacheMap.Force(cacheMap)
//        }
//
//    /// <summary>
//    /// Merge two CloudVectors. This operation returns a new CloudVector that contains the partitions
//    /// of the first CloudVector followed by the partitions of the second CloudVector.
//    /// CacheMaps are combined.
//    /// </summary>
//    /// <param name="v1">First CloudVector.</param>
//    /// <param name="v2">Second CloudVector.</param>
//    member v1.Merge(v2 : CloudVector2<'T>) : Cloud<CloudVector2<'T>> = 
//        cloud {
//            let count = v1.Count + v2.Count
//            let partitions = Array.append v1.Partitions v2.Partitions
//            let! map1, map2 = v1.CacheMap.Value <||> v2.CacheMap.Value
//            let! map = CloudAtom.New(CacheState.Combine(map1, map2))
//            return new CloudVector2<'T>(count, partitions, map) 
//        }
//
//and internal CacheState = 
//    static member Create<'T>(vector : CloudVector2<'T>, workers : IWorkerRef seq) : CacheMap<'T> = 
//        let workers = workers |> Seq.sort |> Seq.toArray
//        let workerCount = workers.Length
//        
//        let map = 
//            vector.Partitions
//            |> Seq.mapi (fun i p -> i, p)
//            |> Seq.groupBy (fun (i, _) -> i % workerCount)
//            |> Seq.map (fun (key, values) -> 
//                    workers.[key], 
//                    values
//                    |> Seq.map snd
//                    |> Seq.toArray)
//            |> Map.ofSeq
//        Some(map :> _)
//
//    static member Combine<'T>(state1 : CacheMap<'T>, state2 : CacheMap<'T>) : CacheMap<'T> =
//        None
//
///// Common operations on CloudVectors.
//type CloudVector =
//    /// <summary>
//    /// Create a new CloudVector from the given input.
//    /// </summary>
//    /// <param name="values">Input sequence.</param>
//    /// <param name="maxPartitionSize">Max partition size.</param>
//    /// <param name="directory">Optional store directory.</param>
//    /// <param name="serializer">Optional serializer.</param>
//    static member New<'T>(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer) : Cloud<CloudVector2<'T>> =
//        cloud {
//            let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize, ?directory = directory, ?serializer = serializer)
//            let count = ref 0L
//            for p in partitions do 
//                let! c = p.Count
//                count := !count + int64 c
//            let! map = CloudAtom.New(None)
//            return new CloudVector2<'T>(count.Value, partitions, map) 
//        }
//
//
//[<AutoOpen>]
//module StoreClientExtensions =
//    open System.Runtime.CompilerServices
//    open MBrace.Continuation
//    open MBrace.Runtime.InMemory
//    
//    /// Common operations on CloudVectors.
//    type CloudVectorClient internal (resources : ResourceRegistry) =
//        let toAsync wf =
//            async {
//                let! ct = Async.CancellationToken
//                return! Cloud.ToAsync(wf, resources, new InMemoryCancellationToken(ct))
//            }
//        let toSync wf = Async.RunSync wf
//
//        /// <summary>
//        /// Create a new cloud array.
//        /// </summary>
//        /// <param name="values">Collection to populate the cloud array with.</param>
//        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
//        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
//        /// <param name="partitionSize">Approximate partition size in bytes.</param>
//        member __.NewAsync(values : seq<'T> , partitionSize, ?directory : string, ?serializer : ISerializer) : Async<CloudVector<'T>> =
//            CloudVector.New(values, partitionSize, ?directory = directory, ?serializer = serializer)
//            |> toAsync
//    
//        /// <summary>
//        /// Create a new cloud array.
//        /// </summary>
//        /// <param name="values">Collection to populate the cloud array with.</param>
//        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
//        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
//        /// <param name="partitionSize">Approximate partition size in bytes.</param>
//        member __.New(values : seq<'T>, partitionSize,  ?directory : string, ?serializer : ISerializer) : CloudVector<'T> =
//            __.NewAsync(values, partitionSize, ?directory = directory, ?serializer = serializer)
//            |> toSync
//    
//    [<Extension>]
//    type MBrace.Client.StoreClient with
//        [<Extension>]
//        /// CloudVector client.
//        member this.CloudVector = new CloudVectorClient(this.Resources)