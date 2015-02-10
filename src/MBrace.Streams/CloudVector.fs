namespace MBrace.Streams

open System.Collections.Generic
open MBrace
open MBrace.Store
open MBrace.Workflows

#nowarn "444"

/// WorkerRef to cached partitions mappings.
type internal CacheMap<'T> = IDictionary<IWorkerRef, CloudSequence<'T> []> option

/// Represents an ordered collection of values stored in CloudSequence partitions.
type CloudVector<'T> internal (count : int64, partitions : CloudSequence<'T> [], cacheMap : ICloudAtom<CacheMap<'T>>) = 
    interface ICloudDisposable with
        member this.Dispose(): Cloud<unit> = 
            cloud {
                do! partitions
                    |> Seq.map Cloud.Dispose
                    |> Cloud.Parallel
                    |> Cloud.Ignore
                do! cacheMap.Dispose()
            }
         
    /// Total number of values stored.
    member val Count = count
    /// Get CloudVector's partitions.
    member val Partitions = partitions
    /// Number of partitions.
    member val PartitionCount = partitions.Length
    /// Get current CacheMap.
    member val CacheMap : ICloudAtom<CacheMap<'T>> = cacheMap

    /// Returns an enumeration of CloudVector's values.
    member __.ToEnumerable () : Cloud<IEnumerable<'T>> =
        cloud {
            return! partitions |> Sequential.lazyCollect (fun p -> p.ToEnumerable())
        }

    /// <summary>
    /// Reset CloudVector's cache-map.
    /// </summary>
    /// <param name="vector">Input CloudVector.</param>
    abstract NoCache : unit -> Cloud<unit>
    default vector.NoCache() = 
        cloud {
            return! vector.CacheMap.Force(None)
        }

    /// <summary>
    /// Calculate a cache-map for the given CloudVector, based on available runtime workers.
    /// This method does not affect InMemory runtimes and is not supported in distributed runtimes 
    /// without worker targeting.
    /// </summary>
    /// <param name="vector">Input CloudVector.</param>
    abstract Cache : unit -> Cloud<unit>
    default vector.Cache() : Cloud<unit> =
        cloud {
            let! context = Cloud.GetSchedulingContext()
            let! isTargetSupported = Cloud.IsTargetedWorkerSupported
            match context with
            | Sequential | ThreadParallel -> 
                do! vector.CacheMap.Force(None)
                return ()
            | Distributed when not isTargetSupported ->
                return failwith "Cannot Cache in runtimes not supporting worker targeting."
            | Distributed ->
                let! workers = Cloud.GetAvailableWorkers()
                let cacheMap = CacheState.Create(vector, workers)
                do! vector.CacheMap.Force(cacheMap)
        }

    /// <summary>
    /// Merge two CloudVectors. This operation returns a new CloudVector that contains the partitions
    /// of the first CloudVector followed by the partitions of the second CloudVector.
    /// CacheMaps are combined.
    /// </summary>
    /// <param name="v1">First CloudVector.</param>
    /// <param name="v2">Second CloudVector.</param>
    member v1.Merge(v2 : CloudVector<'T>) : Cloud<CloudVector<'T>> = 
        cloud {
            let count = v1.Count + v2.Count
            let partitions = Array.append v1.Partitions v2.Partitions
            let! map1, map2 = v1.CacheMap.Value <||> v2.CacheMap.Value
            let! map = CloudAtom.New(CacheState.Combine(map1, map2))
            return new CloudVector<'T>(count, partitions, map) 
        }

and internal CacheState = 
    static member Create<'T>(vector : CloudVector<'T>, workers : IWorkerRef seq) : CacheMap<'T> = 
        let workers = workers |> Seq.sort |> Seq.toArray
        let workerCount = workers.Length
        
        let map = 
            vector.Partitions
            |> Seq.mapi (fun i p -> i, p)
            |> Seq.groupBy (fun (i, _) -> i % workerCount)
            |> Seq.map (fun (key, values) -> 
                    workers.[key], 
                    values
                    |> Seq.map snd
                    |> Seq.toArray)
            |> Map.ofSeq
        Some(map :> _)

    static member Combine<'T>(state1 : CacheMap<'T>, state2 : CacheMap<'T>) : CacheMap<'T> =
        None

/// Common operations on CloudVectors.
type CloudVector =
    /// <summary>
    /// Create a new CloudVector from the given input.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Max partition size.</param>
    /// <param name="directory">Optional store directory.</param>
    /// <param name="serializer">Optional serializer.</param>
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


[<AutoOpen>]
module StoreClientExtensions =
    open System.Runtime.CompilerServices
    open MBrace.Continuation
    open MBrace.Runtime.InMemory
    
    /// Common operations on CloudVectors.
    type CloudVectorClient internal (resources : ResourceRegistry) =
        let toAsync wf =
            async {
                let! ct = Async.CancellationToken
                return! Cloud.ToAsync(wf, resources, new InMemoryCancellationToken(ct))
            }
        let toSync wf = Async.RunSync wf

        /// <summary>
        /// Create a new cloud array.
        /// </summary>
        /// <param name="values">Collection to populate the cloud array with.</param>
        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
        /// <param name="partitionSize">Approximate partition size in bytes.</param>
        member __.NewAsync(values : seq<'T> , partitionSize, ?directory : string, ?serializer : ISerializer) : Async<CloudVector<'T>> =
            CloudVector.New(values, partitionSize, ?directory = directory, ?serializer = serializer)
            |> toAsync
    
        /// <summary>
        /// Create a new cloud array.
        /// </summary>
        /// <param name="values">Collection to populate the cloud array with.</param>
        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
        /// <param name="partitionSize">Approximate partition size in bytes.</param>
        member __.New(values : seq<'T>, partitionSize,  ?directory : string, ?serializer : ISerializer) : CloudVector<'T> =
            __.NewAsync(values, partitionSize, ?directory = directory, ?serializer = serializer)
            |> toSync
    
    [<Extension>]
    type MBrace.Client.StoreClient with
        [<Extension>]
        /// CloudVector client.
        member this.CloudVector = new CloudVectorClient(this.Resources)