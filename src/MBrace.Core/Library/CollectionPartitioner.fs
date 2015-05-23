namespace MBrace.Store.Internals

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Workflows

type CloudCollection private () =

    /// <summary>
    ///     Traverses provided cloud collections for partitions,
    ///     returning their irreducible components while preserving ordering.
    /// </summary>
    /// <param name="collections">Input cloud collections.</param>
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
                let! extracted = cs |> Local.Sequential.map extractCollection
                return Seq.concat extracted
            }

        let! extracted = extractCollections collections
        return Seq.toArray extracted
    }

    /// <summary>
    ///     Traverses provided cloud collection for partitions,
    ///     returning its irreducible components while preserving ordering.
    /// </summary>
    /// <param name="collections">Input cloud collections.</param>
    static member ExtractPartitions (collection : ICloudCollection<'T>) : Local<ICloudCollection<'T> []> = CloudCollection.ExtractPartitions([|collection|])

    /// <summary>
    ///     Performs partitioning of provided irreducible CloudCollections to supplied workers.
    ///     This partitioning scheme takes collection sizes as well as worker capacities into account
    ///     in order to achieve uniformity. It also takes IPartitionableCollection (i.e. dynamically partitionable collections)
    ///     into account when traversing.
    /// </summary>
    /// <param name="collections">Collections to be partitioned.</param>
    /// <param name="workers">Workers to partition among.</param>
    /// <param name="isTargetedWorkerEnabled">Enable targeted (i.e. weighted) worker support. Defaults to true.</param>
    /// <param name="weight">Worker weight function. Default to processor count map.</param>
    static member PartitionBySize (collections : ICloudCollection<'T> [], workers : IWorkerRef [], ?isTargetedWorkerEnabled : bool, ?weight : IWorkerRef -> int) = local {
        let weight = defaultArg weight (fun w -> w.ProcessorCount)
        let isTargetedWorkerEnabled = defaultArg isTargetedWorkerEnabled true

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
                    | (w, _) :: [] -> getSizes ((w, remSize) :: acc) [(w, 0L)] 0L
                    | (w, wsize) :: rest when wsize >= remSize -> getSizes ((w, remSize) :: acc) ((w, wsize - remSize) :: rest) 0L
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
                
                let newCurrWorker, newCurrSize = List.head remWorkers2
                let remWorkers3 = List.tail remWorkers2
                return! aux (List.rev (firstPartition :: intermediatePartitions) @ accPartitions) newCurrWorker newCurrSize [lastPartition] remWorkers3 rc

            // include if remaining capacity is more than half the partition size
            | (w, wsz) :: rw, _, (c, csz) :: rc when remWorkerSize * 2L > csz ->
                let partition = mkPartition currWorker (c :: accWorkerCollections)
                return! aux (partition :: accPartitions) w wsz [] rw rc

            // include if no other collection has been accumulated in current worker and
            // remaining capacity is more than a third of the partition size
            | (w, wsz) :: rw, [], (c, csz) :: rc when remWorkerSize * 3L > csz ->
                let partition = mkPartition currWorker (c :: accWorkerCollections)
                return! aux (partition :: accPartitions) w wsz [] rw rc

            // move partition to next worker otherwise
            | (w, wsz) :: rw, _, _ ->
                let partition = mkPartition currWorker accWorkerCollections
                return! aux (partition :: accPartitions) w wsz [] rw remCollections
        }

        if workers.Length = 0 then return invalidArg "workers" "must be non-empty."
        let isSizeKnown = collections |> Array.forall (fun c -> c.IsKnownSize)
        if not isSizeKnown then
            // size of collections not known a priori, do not take it into account.
            if isTargetedWorkerEnabled then
                return
                    collections
                    |> Array.splitWeighted (workers |> Array.map weight)
                    |> Array.mapi (fun i cs -> workers.[i], cs)
            // partitions according to worker length.
            else
                return
                    collections
                    |> Array.splitByPartitionCount workers.Length
                    |> Array.mapi (fun i cs -> workers.[i], cs)
        else

        // compute size per collection and allocate expected size per worker according to weight.
        let! wsizes = collections |> Local.Sequential.map (fun c -> local { let! sz = c.Size in return c, sz })
        let totalSize = wsizes |> Array.sumBy snd
        let coreCount = workers |> Array.sumBy (fun w -> if isTargetedWorkerEnabled then weight w else 1)
        let sizePerCore = totalSize / int64 coreCount
        let rem = ref <| totalSize % int64 coreCount
        let workers = 
            [
                for w in workers do
                    let deg = if isTargetedWorkerEnabled then int64 (weight w) else 1L
                    let r = min deg !rem
                    rem := !rem - r
                    let size = deg * sizePerCore + r
                    yield (w, size)
            ]

        match workers with
        | [] -> return invalidArg "workers" "Should be non-empty collection."
        | (hWorker, hSize) :: tailW -> return! aux [] hWorker hSize [] tailW (Array.toList wsizes)
    }