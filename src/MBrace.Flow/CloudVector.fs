namespace MBrace.Flow

open System.IO
open System.Runtime.Serialization
open System.Collections.Generic
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Workflows

#nowarn "444"

/// Store Persisted CloudFlow implementation.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudVector<'T> internal (partitions : CloudSequence<'T> []) =
    [<DataMember(Name = "Partitions")>]
    let partitions = partitions

    /// Number of partitions in the vector.
    member __.PartitionCount = partitions.Length

    member __.Item with get i = partitions.[i]

    /// Returns true if in-memory caching support is enabled for the vector instance.
    member __.IsCachingEnabled = partitions |> Array.forall (fun p -> p.CacheByDefault)

    /// Creates an immutable copy iwht updated cache behaviour
    member internal __.WithCacheBehaviour b =
        let partitions' = partitions |> Array.map (CloudSequence.WithCacheBehaviour b)
        new CloudVector<'T>(partitions')

    /// Gets the CloudSequence partitions of the CloudVector
    member __.Partitions = partitions
    /// Computes the size (in bytes) of the CloudVector
    member __.Size: Local<int64> = local { let! sizes = partitions |> Sequential.map (fun p -> p.Size) in return Array.sum sizes }
    /// Computes the element count of the CloudVector
    member __.Count: Local<int64> = local { let! counts = partitions |> Sequential.map (fun p -> p.Count) in return Array.sum counts }
    /// Gets an enumerable for all elements in the CloudVector
    member __.ToEnumerable() = local { let! seqs = partitions |> Sequential.map (fun p -> p.ToEnumerable()) in return Seq.concat seqs }

    interface IPartitionedCollection<'T> with
        member cv.Size: Local<int64> = cv.Size
        member cv.Count: Local<int64> = cv.Count
        member cv.GetPartitions(): Local<ICloudCollection<'T> []> = local { return partitions |> Array.map (fun p -> p :> _) }
        member cv.PartitionCount: Local<int> = local { return partitions.Length }
        member cv.ToEnumerable(): Local<seq<'T>> = cv.ToEnumerable()

    interface CloudFlow<'T> with
        member cv.DegreeOfParallelism = None
        member cv.Apply(collectorf : Local<Collector<'T,'S>>) (projection: 'S -> Local<'R>) (combiner: 'R [] -> Local<'R>): Cloud<'R> = cloud {
            // TODO : use ad-hoc implementation with better scheduling
            let flow = CloudCollection.ToCloudFlow(cv, useCache = cv.IsCachingEnabled)
            return! flow.Apply collectorf projection combiner
        }

    interface ICloudDisposable with
        member __.Dispose () = local {
            for p in partitions do do! (p :> ICloudDisposable).Dispose()
        }

    override __.ToString() = sprintf "CloudVector[%O] of %d partitions." typeof<'T> partitions.Length
    member private __.StructuredFormatDisplay = __.ToString()
        

type CloudVector private () =

    /// Maximum CloudVector partition size used in CloudVector.New
    static let MaxCloudVectorPartitionSize = 1024L * 1024L * 1024L // 1GB

    /// <summary>
    ///     Creates a new CloudVector instance out of given sequence.
    /// </summary>
    /// <param name="elems">Input sequence.</param>
    /// <param name="cache">Enable caching behaviour in CloudVector instance.</param>
    /// <param name="partitionThreshold">Partition threshold in bytes.</param>
    static member internal New(elems : seq<'T>, cache : bool, ?partitionThreshold:int64) : Local<CloudVector<'T>> = local {
        let partitionThreshold = defaultArg partitionThreshold MaxCloudVectorPartitionSize
        let! cseqs = CloudSequence.NewPartitioned(elems, partitionThreshold, enableCache = cache) 
        return new CloudVector<'T>(cseqs)
    }
    
    /// <summary>
    ///     Concatenates a series of CloudVectors into one.
    /// </summary>
    /// <param name="vectors">Input CloudVectors.</param>
    static member Concat (vectors : seq<CloudVector<'T>>) : CloudVector<'T> = 
        let partitions = vectors |> Seq.collect (fun v -> v.Partitions) |> Seq.toArray
        new CloudVector<'T>(partitions)

    /// <summary>
    ///     Converts a CloudVector to cloud flow.
    /// </summary>
    /// <param name="vector">Input Cloud Vector.</param>
    static member internal ToCloudFlow (vector : CloudVector<'T>) : CloudFlow<'T> = vector :> CloudFlow<'T>

    /// <summary>
    ///     Creates a CloudVector by persisting given flow to store.
    /// </summary>
    /// <param name="flow">Input CloudFlow.</param>
    /// <param name="enableCache">Use in-memory caching as vector is created.</param>
    static member internal OfCloudFlow (flow : CloudFlow<'T>, enableCache : bool) : Cloud<CloudVector<'T>> = cloud {
        match flow with
        | :? CloudVector<'T> as cv -> return cv.WithCacheBehaviour enableCache
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
                        let mutable counter = -1
                        for list in results do
                            for i = 0 to list.Count - 1 do
                                let value = list.[i]
                                counter <- counter + 1
                                values.[counter] <- value
                        values }
            }

            let! cts = Cloud.CreateCancellationTokenSource()
            let createVector (ts : 'T []) = local {
                let! vector = CloudVector.New(ts, cache = enableCache)
                if enableCache then
                    let! objCache = Cloud.TryGetResource<IObjectCache> ()
                    match objCache with
                    | None -> ()
                    | Some oc ->
                        // inject result array directly to cache
                        let i = ref 0
                        for p in vector.Partitions do
                            let! count = p.Count
                            let j = !i + int count
                            let sub = ts.[!i .. j - 1]
                            let _ = oc.Add((p :> ICloudCacheable<'T[]>).UUID, sub)
                            i := j

                return vector
            }

            return! flow.Apply (collectorf cts) createVector (fun result -> local { return CloudVector.Concat result })
    }