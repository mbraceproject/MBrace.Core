namespace MBrace.Flow

open System.IO
open System.Runtime.Serialization
open System.Collections.Generic
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "444"

/// Persisted CloudFlow implementation.
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type PersistedCloudFlow<'T> internal (storageLevel : StorageLevel, partitions : (int64 * ICloudValue<'T []>) []) =

    [<DataMember(Name = "StorageLevel")>]
    let storageLevel = storageLevel
    [<DataMember(Name = "Partitions")>]
    let partitions = partitions

    member internal __.Partitions = partitions

    /// Number of partitions in the vector.
    member __.PartitionCount = partitions.Length

    member __.Item with get i = snd partitions.[i]

    member __.StorageLevel = storageLevel

    /// Gets the CloudSequence partitions of the PersistedCloudFlow
    member internal __.Partitions = partitions
    /// Computes the size (in bytes) of the PersistedCloudFlow
    member __.Size: int64 = partitions |> Array.sumBy (fun (_,p) -> p.Size)
    /// Computes the element count of the PersistedCloudFlow
    member __.Count: int64 = partitions |> Array.sumBy fst
    /// Gets an enumerable for all elements in the PersistedCloudFlow
    member __.ToEnumerable() : seq<'T> = seq {
        for _,cv in partitions do
            yield! cv.Value
    }

    interface IPartitionedCollection<'T> with
        member cv.IsKnownSize = true
        member cv.IsKnownCount = true
        member cv.Size: Async<int64> = async { return cv.Size }
        member cv.Count: Async<int64> = async { return cv.Count }
        member cv.GetPartitions(): Async<ICloudCollection<'T> []> = async { return partitions |> Array.map (fun fs -> new PersistedCloudFlow<'T>(storageLevel, [|fs|]) :> _) }
        member cv.PartitionCount: Async<int> = async { return partitions.Length }
        member cv.ToEnumerable(): Async<seq<'T>> = async { return cv.ToEnumerable() }

    interface CloudFlow<'T> with
        member cv.DegreeOfParallelism = None
        member cv.WithEvaluators(collectorf : Local<Collector<'T,'S>>) (projection: 'S -> Local<'R>) (combiner: 'R [] -> Local<'R>): Cloud<'R> = cloud {
            // TODO : use ad-hoc implementation with better scheduling
            let flow = CloudCollection.ToCloudFlow(cv)
            return! flow.WithEvaluators collectorf projection combiner
        }

    interface ICloudDisposable with
        member __.Dispose () = async {
            for _,p in partitions do do! (p :> ICloudDisposable).Dispose()
        }

    override __.ToString() = sprintf "PersistedCloudFlow[%O] of %d partitions." typeof<'T> partitions.Length
    member private __.StructuredFormatDisplay = __.ToString()
        

type internal PersistedCloudFlow private () =

    /// <summary>
    ///     Creates a new persisted CloudFlow instance out of given enumeration.
    /// </summary>
    /// <param name="elems">Input sequence.</param>
    /// <param name="cache">Enable caching behaviour in PersistedCloudFlow instance.</param>
    static member New(elems : 'T [], cache : bool, ?partitionThreshold:int64) : Local<PersistedCloudFlow<'T>> = local {
        let! cvalue = CloudValue.New(elems)
        return new PersistedCloudFlow<'T>(cvalue.StorageLevel, [|elems.LongLength, cvalue|])
    }
    
    /// <summary>
    ///     Concatenates a series of persisted CloudFlows into one.
    /// </summary>
    /// <param name="vectors">Input CloudFlows.</param>
    static member Concat (vectors : seq<PersistedCloudFlow<'T>>) : PersistedCloudFlow<'T> = 
        let partitions = vectors |> Seq.collect (fun v -> v.Partitions) |> Seq.toArray
        let storageLevel = partitions |> Array.fold (fun s (_,cv) -> cv.StorageLevel &&& s) (* TODO: fix *)(snd(partitions.[0]).StorageLevel)
        new PersistedCloudFlow<'T>(storageLevel, partitions)

    /// <summary>
    ///     Persists given flow to store.
    /// </summary>
    /// <param name="flow">Input CloudFlow.</param>
    /// <param name="enableCache">Use in-memory caching as vector is created.</param>
    static member Persist (flow : CloudFlow<'T>, enableCache : bool) : Cloud<PersistedCloudFlow<'T>> = cloud {
        match flow with
        | :? PersistedCloudFlow<'T> as cv -> return cv.WithCacheBehaviour enableCache
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
                let! vector = PersistedCloudFlow.New(ts, cache = enableCache)
                // TODO: Revisit in-memory caching effect
                return vector
            }

            return! flow.WithEvaluators (collectorf cts) createVector (fun result -> local { return PersistedCloudFlow.Concat result })
    }