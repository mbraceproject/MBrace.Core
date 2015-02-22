namespace MBrace.Streams

open System.IO
open System.Collections.Generic

open MBrace
open MBrace.Store
open MBrace.Workflows

#nowarn "444"

/// Represents the cached indices corresponding to each worker node of the cluster
type WorkerCacheState = IWorkerRef * int []

/// Represents an ordered collection of values stored in CloudSequence partitions.
[<AbstractClass>]
type CloudVector<'T> () =
    /// Gets the total element count for the cloud vector.
    abstract Count : Cloud<int64>
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
    abstract ToEnumerable : unit -> Cloud<seq<'T>>
    /// Gets the cache support status for cloud vector instance.
    abstract IsCachingSupported : bool
    /// Gets the current cache state of the vector inside the cluster.
    abstract GetCacheState : unit -> Cloud<WorkerCacheState []>
    /// Updates the cache state to include provided indices for given worker ref.
    abstract UpdateCacheState : worker:IWorkerRef * appendedIndices:int[] -> Cloud<unit>
    /// Disposes cloud vector from store.
    abstract Dispose : unit -> Cloud<unit>

    interface ICloudDisposable with
        member __.Dispose () = __.Dispose()

/// Cloud vector implementation that keeps cache information in a single cloud atom
type internal AtomCloudVector<'T>(elementCount : int64 option, partitions : CloudSequence<'T> [], cacheMap : ICloudAtom<Map<IWorkerRef, int[]>> option) =
    inherit CloudVector<'T> ()

    let mutable elementCount = elementCount

    let getCacheMap() =
        match cacheMap with
        | None -> raise <| new System.NotSupportedException("caching")
        | Some cm -> cm
        

    override __.Count = cloud {
        match elementCount with
        | Some c -> return c
        | None ->
            let! counts =
                partitions
                |> Seq.map (fun p -> p.Count)
                |> Cloud.Parallel
                |> Cloud.ToLocal

            let count = counts |> Array.sumBy int64
            elementCount <- Some count
            return count
    }

    override __.PartitionCount = partitions.Length
    override __.GetAllPartitions () = partitions
    override __.GetPartition i = partitions.[i]
    override __.Item with get i = partitions.[i]
    override __.ToEnumerable() = cloud {
        return! partitions |> Sequential.lazyCollect (fun p -> p.ToEnumerable())
    }

    override __.IsCachingSupported = Option.isSome cacheMap
    override __.GetCacheState () = getCacheMap().Value |> Cloud.map (fun m -> m |> Seq.map (function KeyValue(w,is) -> (w,is)) |> Seq.toArray)

    override __.UpdateCacheState(worker : IWorkerRef, appendedIndices : int []) = cloud {
        let cacheMap = getCacheMap()
        let updater (state : Map<IWorkerRef, int[]>) =
            let indices =
                match state.TryFind worker with
                | None -> appendedIndices
                | Some is -> Seq.append is appendedIndices |> Seq.distinct |> Seq.toArray

            state.Add(worker, indices)

        return! cacheMap.Update updater
    }

    override __.Dispose() = cloud {
        return!
            partitions
            |> Array.map (fun p -> (p :> ICloudDisposable).Dispose())
            |> Cloud.Parallel
            |> Cloud.ToLocal
            |> Cloud.Ignore
    }

/// Cloud vector implementation that results from concatennation of multiple cloudvectors
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

    override __.Count = cloud {
        let! counts = components |> Sequential.map (fun c -> c.Count)
        return Array.sum counts
    }

    override __.PartitionCount = components |> Array.sumBy(fun c -> c.PartitionCount)
    override __.ToEnumerable() = cloud {
        return! components |> Sequential.lazyCollect(fun p -> p.ToEnumerable())
    }

    override __.GetAllPartitions () = components |> Array.collect(fun c -> c.GetAllPartitions())
    override __.GetPartition i = let ci, pi = global2Local i in components.[ci].[pi]
    override __.Item with get i = let ci, pi = global2Local i in components.[ci].[pi]

    override __.IsCachingSupported = components |> Array.forall(fun c -> c.IsCachingSupported)
    override __.GetCacheState() = cloud {
        let getComponentCacheState (ci : int) (c : CloudVector<'T>) = cloud {
            let! state = c.GetCacheState()
            // transform indices before returning
            return state |> Array.map (fun (w,is) -> w, is |> Array.map (fun i -> local2Global (ci, i)))
        }
            
        let! states =
            components
            |> Seq.mapi getComponentCacheState
            |> Cloud.Parallel
            |> Cloud.ToLocal

        return
            states
            |> Seq.concat
            |> Seq.groupBy fst
            |> Seq.map (fun (w, css) -> w, css |> Seq.collect (fun (_, is) -> is) |> Seq.sort |> Seq.toArray)
            |> Seq.toArray
    }

    override __.UpdateCacheState(w : IWorkerRef, indices : int[]) = cloud {
        let groupedIndices =
            indices
            |> Seq.map global2Local
            |> Seq.groupBy fst
            |> Seq.map (fun (ci, iss) -> ci, iss |> Seq.map snd |> Seq.toArray)
            |> Seq.toArray

        do!
            groupedIndices
            |> Seq.map (fun (ci, iss) -> components.[ci].UpdateCacheState(w, iss))
            |> Cloud.Parallel
            |> Cloud.ToLocal
            |> Cloud.Ignore
    }

    override __.Dispose () = cloud {
        do!
            components
            |> Seq.map (fun c -> c.Dispose())
            |> Cloud.Parallel
            |> Cloud.ToLocal
            |> Cloud.Ignore
    }
            
/// Cloud vector static API
type CloudVector =

    /// <summary>
    ///     Creates a new CloudVector out of a collection of CloudSequence partitinons
    /// </summary>
    /// <param name="partitions">CloudSequences that constitute the vector.</param>
    /// <param name="enableCaching">Enable in-memory caching of partitions in worker roles. Defaults to true.</param>
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

    /// <summary>
    ///     Creates a cloudvector instance using a collection of cloud files and provided deserializer method.
    /// </summary>
    /// <param name="files">Input file paths.</param>
    /// <param name="deserializer">Deserializer lambda for given file.</param>
    /// <param name="enableCaching">Enable in-memory caching for CloudVector instance. Defaults to true.</param>
    static member OfCloudFiles(files : seq<string>, deserializer : Stream -> seq<'T>, ?enableCaching) = cloud {
        let! partitions = 
            files 
            |> Seq.map (fun f -> CloudSequence.FromFile(f, deserializer, force = false))
            |> Cloud.Parallel
            |> Cloud.ToLocal

        return! CloudVector.OfPartitions(partitions, ?enableCaching = enableCaching)
    }

    /// <summary>
    ///     Merge a collection of cloud vectors into a single instance.
    /// </summary>
    /// <param name="components">CloudVector components.</param>
    static member Merge(components : seq<CloudVector<'T>>) : CloudVector<'T> =
        let components = 
            components
            |> Seq.collect (function :? ConcatenatedCloudVector<'T> as c -> c.Components | v -> [|v|])
            |> Seq.toArray

        new ConcatenatedCloudVector<'T>(components) :> CloudVector<'T>

    
    /// <summary>
    ///     Creates a CloudVector in file store using a collection of sequences with specified partition size.
    /// </summary>
    /// <param name="values">Inputs values for cloud vector.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes for each vector partition in file store.</param>
    /// <param name="enableCaching">Enable caching for cloud vector instance. Defaults to true.</param>
    static member New<'T>(values : seq<'T>, maxPartitionSize : int64, ?enableCaching:bool) : Cloud<CloudVector<'T>> = cloud {
        let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize)
        return! CloudVector.OfPartitions(partitions, ?enableCaching = enableCaching)
    }