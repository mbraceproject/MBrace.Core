namespace MBrace.Store.Internals

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Workflows

type CloudCollection private () =

    static member ExtractPartitions (collections : seq<ICloudCollection<'T>>) : Local<ICloudCollection<'T> []> = local {
        let rec extractCollection (c : ICloudCollection<'T>) = 
            local {
                match c with
                | :? IPartitionedCollection<'T> as c ->
                    let! partitions = c.GetPartitions()
                    return! extractCollections partitions
                | c -> return Seq.singleton c
            }

        and extractCollections (cs : seq<ICloudCollection<'T>>) =
            local {
                let! extracted = cs |> Sequential.map extractCollection
                return Seq.concat extracted
            }

        let! extracted = extractCollections collections
        return Seq.toArray extracted
    }

    static member ExtractPartitions (collection : ICloudCollection<'T>) : Local<ICloudCollection<'T> []> = CloudCollection.ExtractPartitions([|collection|])

    static member PartitionBySize (workers : IWorkerRef [], isTargetedWorkerEnabled : bool, collections : ICloudCollection<'T> []) = local {
        if workers.Length = 0 then return invalidArg "workers" "must be non-empty."
        let isSizeKnown = collections |> Array.forall (fun c -> c.IsKnownSize)
        if not isSizeKnown then
            if isTargetedWorkerEnabled then
                return WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers collections
            else
                return WorkerRef.partition workers collections
        else
            
        let rec aux (accPartitions : (IWorkerRef * ICloudCollection<'T> []) list) 
                    (currWorker : IWorkerRef) (remWorkerSize : int64) (accWorkerCollections : ICloudCollection<'T> list)
                    (remWorkers : (IWorkerRef * int64) list) (remCollections : (ICloudCollection<'T> * int64) list) = local {

            let mkPartition worker (acc : ICloudCollection<'T> list) = worker, acc |> List.rev |> List.toArray

            match remWorkers, accWorkerCollections, remCollections with
            // remaining collection set exhausted, return accumulated partitions with empty sets for remaining workers.
            | _, _, [] ->
                return [|
                    yield! List.rev accPartitions
                    yield mkPartition currWorker accWorkerCollections
                    for rw,_ in remWorkers -> (rw, [||]) |]

            // remaining worker set exhausted, shoehorn all remaining collections to the current worker.
            | [], awc, rcs -> 
                let rcs = rcs |> List.map fst |> List.rev
                return! aux accPartitions currWorker 0L (rcs @ awc) [] []

            // next collection is withing remaining worker size, include to accumulated collections and update size.
            | _, _, (c, csz) :: rc when csz <= remWorkerSize -> 
                return! aux accPartitions currWorker (remWorkerSize - csz) (c :: accWorkerCollections) remWorkers rc

            // next collection is partitionable that does not fit in current worker, begin dynamic partitioning logic.
            | _, _, (:? IPartitionableCollection<'T> as pc, csz) :: rc ->
                // traverse remaining workers, computing size of partitionable allocated to each of them.
                let rec getSizes (acc : (IWorkerRef * int64) list) (workers : (IWorkerRef * int64) list) (remSize : int64) =
                    if remSize <= 0L then List.rev acc, workers else

                    match workers with
                    | [] -> failwith "CloudCollection.PartitionBySize: internal error."
                    | (w, wsize) :: rest when wsize > remSize -> getSizes ((w, remSize) :: acc) ((w, wsize - remSize) :: rest) 0L
                    | (_, wsize) as w :: rest -> getSizes (w :: acc) rest (remSize - wsize)

                let sizes, remWorkers2 = getSizes [] ((currWorker, remWorkerSize) :: remWorkers) csz

                // compute partition weights based on calculated worker sizes
                let weights =
                    let sizes = sizes |> Seq.map snd |> Seq.toArray
                    let gcdNormalized = Array.gcdNormalize sizes
                    let max = Array.max gcdNormalized
                    if max <= int64 Int32.MaxValue then gcdNormalized |> Array.map int
                    else
                        let min = gcdNormalized |> Array.min |> decimal
                        [| for s in gcdNormalized -> (decimal s / min) |> round |> int |]

                // extract partitions based on weights
                let! cpartitions = pc.GetPartitions weights

                // Partition array should contain at least 2 elements:
                // * the first partition is assigned to the current worker.
                // * the last partition will be included in the accumulator state at the tail call.
                // * intermediate partitions, if they exist, will be included as standalone collections
                //   in their assigned workers .

                let firstPartition = mkPartition currWorker (cpartitions.[0] :: accWorkerCollections)
                let lastPartition = cpartitions.[cpartitions.Length - 1]
                let intermediatePartitions =
                    [
                        for i = 1 to cpartitions.Length - 2 do
                            let w,_ = remWorkers.[i - 1] 
                            yield (w, [| cpartitions.[i] |])
                    ]
                
                let newCurrWorker, newCurrSize = remWorkers.[cpartitions.Length - 2]
                return! aux (List.rev (firstPartition :: intermediatePartitions) @ accPartitions) newCurrWorker newCurrSize [lastPartition] remWorkers2 rc

            // include if remaining capacity is more than half the partition size
            | (w, wsz) :: rw, _, (c, csz) :: rc when remWorkerSize * 2L > csz ->
                let partition = mkPartition currWorker (c :: accWorkerCollections)
                return! aux (partition :: accPartitions) w wsz [] rw rc
            // move partition to next worker otherwise
            | (w, wsz) :: rw, _, _ ->
                let partition = mkPartition currWorker accWorkerCollections
                return! aux (partition :: accPartitions) w wsz [] rw remCollections
        }

        let! wsizes = collections |> Sequential.map (fun c -> local { let! sz = c.Size in return c, sz })
        let totalSize = wsizes |> Array.sumBy snd
        let coreCount = workers |> Array.sumBy (fun w -> if isTargetedWorkerEnabled then w.ProcessorCount else 1)
        let sizePerCore = totalSize / int64 coreCount
        let rem = ref <| totalSize % int64 coreCount
        let workers = 
            [
                for w in workers do
                    let deg = if isTargetedWorkerEnabled then int64 w.ProcessorCount else 1L
                    let r = min deg !rem
                    rem := !rem - r
                    let size = deg * sizePerCore + r
                    yield (w, size)
            ]

        match workers with
        | [] -> return invalidArg "workers" "Should be non-empty collection."
        | (hWorker, hSize) :: tailW -> return! aux [] hWorker hSize [] tailW (Array.toList wsizes)
    }