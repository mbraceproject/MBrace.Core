namespace MBrace.Flow

open System
open System.IO
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "444"

/// Persisted CloudFlow implementation.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type PersistedCloudFlow<'T> internal (partitions : (IWorkerRef * ICloudArray<'T>) []) =

    [<DataMember(Name = "Partitions")>]
    let partitions = partitions

    /// Number of partitions in the vector.
    member __.PartitionCount = partitions.Length

    member __.Item with get i = partitions.[i]

    /// Caching level for persisted data
    member __.StorageLevel = 
        if partitions.Length = 0 then StorageLevel.Memory
        else
            let _,ca = partitions.[0]
            partitions |> Array.fold (fun s (_,cv) -> s &&& cv.StorageLevel) ca.StorageLevel

    /// Indicates whether this CloudFlow instance is available for consumption from the local context.
    member __.IsAvailableLocally : bool =
        let isAvailablePartition (_, cv : ICloudValue) =
            cv.StorageLevel.HasFlag StorageLevel.Disk || cv.IsCachedLocally

        partitions |> Array.forall isAvailablePartition

    /// Gets the CloudSequence partitions of the PersistedCloudFlow
    member __.Partitions = partitions
    /// Computes the size (in bytes) of the PersistedCloudFlow
    member __.Size: int64 = partitions |> Array.sumBy (fun (_,p) -> p.Size)
    /// Computes the element count of the PersistedCloudFlow
    member __.Count: int64 = partitions |> Array.sumBy (fun (_,p) -> int64 p.Length)

    /// Gets an enumerable for all elements in the PersistedCloudFlow
    member private __.ToEnumerable() : seq<'T> =
        match partitions with
        | [||] -> Seq.empty
        | [| (_,p) |] -> p.Value :> seq<'T>
        | _ -> partitions |> Seq.collect snd

    interface seq<'T> with
        member x.GetEnumerator() = x.ToEnumerable().GetEnumerator() :> IEnumerator
        member x.GetEnumerator() = x.ToEnumerable().GetEnumerator()

    interface ITargetedPartitionCollection<'T> with
        member cv.IsKnownSize = true
        member cv.IsKnownCount = true
        member cv.IsMaterialized = false
        member cv.GetSize(): Async<int64> = async { return cv.Size }
        member cv.GetCount(): Async<int64> = async { return cv.Count }
        member cv.GetPartitions(): Async<ICloudCollection<'T> []> = async { return partitions |> Array.map (fun (_,p) -> p :> ICloudCollection<'T>) }
        member cv.GetTargetedPartitions() :Async<(IWorkerRef * ICloudCollection<'T>) []> = async { return partitions |> Array.map (fun (w,ca) -> w, ca :> _) }
        member cv.PartitionCount: Async<int> = async { return partitions.Length }
        member cv.ToEnumerable() = async { return cv.ToEnumerable() }

    interface CloudFlow<'T> with
        member cv.DegreeOfParallelism = None
        member cv.WithEvaluators(collectorf : Local<Collector<'T,'S>>) (projection: 'S -> Local<'R>) (combiner: 'R [] -> Local<'R>): Cloud<'R> = cloud {
            let flow = CloudCollection.ToCloudFlow(cv)
            return! flow.WithEvaluators collectorf projection combiner
        }

    interface ICloudDisposable with
        member __.Dispose () = async {
            return!
                partitions
                |> Array.map (fun (_,p) -> (p :> ICloudDisposable).Dispose())
                |> Async.Parallel
                |> Async.Ignore
        }

    override __.ToString() = sprintf "PersistedCloudFlow[%O] of %d partitions." typeof<'T> partitions.Length
    member private __.StructuredFormatDisplay = __.ToString()
        

type PersistedCloudFlow private () =

    static let defaultTreshold = 1024L * 1024L * 1024L

    /// <summary>
    ///     Creates a new persisted CloudFlow instance out of given enumeration.
    /// </summary>
    /// <param name="elems">Input sequence.</param>
    /// <param name="elems">Storage level used for caching.</param>
    /// <param name="partitionThreshold">Partition threshold in bytes. Defaults to 1GB.</param>
    static member internal New(elems : seq<'T>, ?storageLevel : StorageLevel, ?partitionThreshold : int64) : Local<PersistedCloudFlow<'T>> = local {
        let partitionThreshold = defaultArg partitionThreshold defaultTreshold
        let! currentWorker = Cloud.CurrentWorker
        let! partitions = CloudValue.NewPartitioned(elems, ?storageLevel = storageLevel, partitionThreshold = partitionThreshold)
        return new PersistedCloudFlow<'T>(partitions |> Array.map (fun p -> (currentWorker,p)))
    }

    /// <summary>
    ///     Creates a CloudFlow from a collection of provided cloud sequences.
    /// </summary>
    /// <param name="cloudArrays">Cloud sequences to be evaluated.</param>
    static member OfCloudArrays (cloudArrays : seq<#ICloudArray<'T>>) : Local<PersistedCloudFlow<'T>> = local {
        let! workers = Cloud.GetAvailableWorkers()
        let partitions = cloudArrays |> Seq.mapi (fun i ca -> workers.[i % workers.Length], ca :> ICloudArray<'T>) |> Seq.toArray
        return new PersistedCloudFlow<'T>(Seq.toArray partitions)
    }
    
    /// <summary>
    ///     Concatenates a series of persisted CloudFlows into one.
    /// </summary>
    /// <param name="vectors">Input CloudFlows.</param>
    static member Concat (vectors : seq<PersistedCloudFlow<'T>>) : PersistedCloudFlow<'T> = 
        let partitions = vectors |> Seq.collect (fun v -> v.Partitions) |> Seq.toArray
        new PersistedCloudFlow<'T>(partitions)

    /// <summary>
    ///     Persists given flow to store.
    /// </summary>
    /// <param name="flow">Input CloudFlow.</param>
    /// <param name="flow">StorageLevel to be used. Defaults to implementation default.</param>
    static member Persist (flow : CloudFlow<'T>, ?storageLevel : StorageLevel) : Cloud<PersistedCloudFlow<'T>> = cloud {
        let! defaultLevel = CloudValue.DefaultStorageLevel
        let storageLevel = defaultArg storageLevel defaultLevel
        match flow with
        // TODO : storage level update semantics feels wrong; check later
        | :? PersistedCloudFlow<'T> as cv when cv.StorageLevel.HasFlag defaultLevel -> return cv
        | _ ->
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
                        let mutable counter = 0
                        for list in results do
                            for i = 0 to list.Count - 1 do
                                let value = list.[i]
                                values.[counter] <- value
                                counter <- counter + 1
                        values }
            }

            let! cts = Cloud.CreateCancellationTokenSource()
            let createVector (ts : 'T []) = PersistedCloudFlow.New(ts, storageLevel = storageLevel)
            return! flow.WithEvaluators (collectorf cts) createVector (fun result -> local { return PersistedCloudFlow.Concat result })
    }