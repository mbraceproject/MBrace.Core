namespace MBrace.Streams

open System
open System.Collections
open System.Collections.Generic

open MBrace.Store
open MBrace.Continuation
open MBrace
open Nessos.Streams
open System.Runtime.Serialization

type internal CacheState = Map<IWorkerRef, int []>

[<AbstractClass>]
type CloudVector<'T> internal () =
    abstract PartitionCount : int
    abstract GetPartition : index:int -> CloudSequence<'T>

type internal AtomicCloudVector<'T> (partitions : CloudSequence<'T> [], globalCacheState : ICloudAtom<CacheState>) =
    inherit CloudVector<'T> ()
    
    override c.PartitionCount = partitions.Length
    override c.GetPartition(i : int) = partitions.[i]

    member c.CachePartitions(indeces : int []) = cloud {
        let! worker = Cloud.CurrentWorker
        // cache partitions
        
        let ps = indeces |> Array.map (fun i -> partitions.[i]) // force IndexOutOfRange before any caching takes place
        for p in ps do
            do! Cloud.Ignore(p.Cache())

        // update global state
        let update (currentState : CacheState) : CacheState =
            let mutable localState = defaultArg (currentState.TryFind worker) [||] |> Set.ofArray
            for i in indeces do
                localState <- Set.add i localState

            Map.add worker (Set.toArray localState) currentState

        do! CloudAtom.Update(globalCacheState, update)
    }

    member c.GetCacheState () = CloudAtom.Read globalCacheState

type internal MergedCloudVector<'T>(merged : AtomicCloudVector<'T> []) =
    inherit CloudVector<'T>()

    let transformIndex (index : int) = 
        let rec lookup offset i =
            if i = merged.Length then raise <| IndexOutOfRangeException()
            elif index - offset < merged.[i].PartitionCount then
                i, index - offset
            else
                lookup (offset + merged.[i].PartitionCount) (i+1)

        lookup 0 0

    override c.PartitionCount = merged |> Array.sumBy (fun acv -> acv.PartitionCount)
    override c.GetPartition(index : int) = let p,i = transformIndex index in merged.[p].GetPartition i
    member c.Components = merged

    member c.CachePartitions (indeces : int []) = cloud {
        let transformed = indeces |> Seq.map transformIndex |> Seq.groupBy fst  |> Seq.toArray
        for p,is in transformed do
            do! merged.[p].CachePartitions(is |> Seq.map snd |> Seq.toArray)
    }

    member c.GetCacheState() : Cloud<CacheState> = cloud {
        let! cacheStates = merged |> Seq.map (fun m -> m.GetCacheState()) |> Cloud.Parallel |> Cloud.ToLocal
        return
            seq {
                let offset = ref 0
                for i in 0 .. merged.Length - 1 do
                    for KeyValue(w, js) in cacheStates.[i] do
                        for j in js -> w, j + !offset

                    offset := !offset + merged.[i].PartitionCount
            } |> Seq.groupBy fst |> Seq.map (fun (w,js) -> w, js |> Seq.map snd |> Seq.toArray) |> Map.ofSeq
    }


type CloudVector private () =

    static let getComponents (vector : CloudVector<'T>) =
        match vector with 
        | :? AtomicCloudVector<'T> as v -> [| v |] 
        | :? MergedCloudVector<'T> as v -> v.Components
        | _ -> invalidOp "impossible"
    
    static member New(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer) : Cloud<CloudVector<'T>> = cloud {
        let! partitions = CloudSequence.NewPartitioned(values, maxPartitionSize, ?directory = directory, ?serializer = serializer)
        let! cacheState = CloudAtom.New(Map.empty)
        return new AtomicCloudVector<'T>(partitions, cacheState) :> _
    }

    static member Append(v1 : CloudVector<'T>, v2 : CloudVector<'T>) : CloudVector<'T> =
        new MergedCloudVector<'T>(Array.append (getComponents v1) (getComponents v2)) :> _

    static member Append(vectors : seq<CloudVector<'T>>) : CloudVector<'T> =
        let components =
            vectors
            |> Seq.map getComponents
            |> Seq.concat
            |> Seq.toArray

        new MergedCloudVector<'T>(components) :> _
//
//#nowarn "444"
//
///// Provides methods on CloudArrays.
//type CloudArray =
//    
//    /// <summary>
//    /// Create a new cloud array.
//    /// </summary>
//    /// <param name="values">Collection to populate the cloud array with.</param>
//    /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
//    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
//    /// <param name="partitionSize">Approximate partition size in bytes.</param>
//    static member New(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) = cloud {
//        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
//        
//        let directory = match directory with None -> config.DefaultDirectory | Some d -> d
//
//        return! Cloud.OfAsync <| CloudArray<'T>.CreateAsync(values, directory, config, ?serializer = serializer, ?partitionSize = partitionSize)
//    }
//
//
//[<AutoOpen>]
//module StoreClientExtensions =
//    open System.Runtime.CompilerServices
//    
//    /// Common operations on CloudArrays.
//    type CloudArrayClient internal (resources : ResourceRegistry) =
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
//            CloudArray.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
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
//            CloudArray.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
//            |> toSync
//    
//    [<Extension>]
//    type MBrace.Client.StoreClient with
//        [<Extension>]
//        /// CloudArray client.
//        member this.CloudArray = new CloudArrayClient(this.Resources)