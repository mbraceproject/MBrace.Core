namespace MBrace.Store.Internals

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

    static member PartitionBySize (workers : IWorkerRef [],  isTargetedWorkerEnabled : bool, collections : ICloudCollection<'T> []) = local {
        let isSizeKnown = collections |> Array.forall (fun c -> c.IsKnownSize)
        if not isSizeKnown then
            if isTargetedWorkerEnabled then
                return WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers collections
            else
                return WorkerRef.partition workers collections
        else

        let! sizes = collections |> Sequential.map (fun c -> c.Size)
        let totalSize = Array.sum sizes
        let coreCount = workers |> Array.sumBy (fun w -> if isTargetedWorkerEnabled then w.ProcessorCount else 1)
        let sizePerCore = totalSize / int64 coreCount
        let sizesPerWorker = [| for w in workers -> if isTargetedWorkerEnabled then sizePerCore else int64 w.ProcessorCount * sizePerCore |]

        let rec aux acc accWorker (accWorkerSize : int64) (workerIndex : int) (collectionSlices : (int * int64) list) (collectionIndex : int) = local {
            let mkPartition i (collected : ICloudCollection<'T> list) = workers.[i], collected |> List.rev |> List.toArray
            if collectionIndex = collections.Length then 
                match accWorker with
                | [] -> return acc |> List.rev |> List.toArray
                | _ -> return mkPartition workerIndex accWorker :: acc |> List.rev |> List.toArray
            else
                match [], collections.[collectionIndex] with
                | [], collection when accWorkerSize + sizes.[collectionIndex] <= sizesPerWorker.[workerIndex] ->
                    return! aux acc (collection :: accWorker) (accWorkerSize + sizes.[collectionIndex]) workerIndex [] (collectionIndex + 1)

                | _, (:? IPartitionableCollection<'T> as pc) ->
                    let usedBytes = collectionSlices |> List.sumBy snd
                    let remainingBytes = sizes.[collectionIndex] - usedBytes
                    if accWorkerSize + remainingBytes <= sizesPerWorker.[workerIndex] then
                        let slices = (workerIndex, remainingBytes) :: collectionSlices |> List.rev |> List.toArray
                        let weights = slices |> Array.map snd |> Array.gcdNormalize |> Array.map int
                        let! partitions = pc.GetPartitions weights
                        assert(weights.Length > 1 && partitions.Length = weights.Length)
                        let accPartitions =
                            [
                                yield mkPartition (fst slices.[0]) (partitions.[0] :: accWorker)
                                for i in 1 .. partitions.Length - 1 -> mkPartition (fst slices.[i]) [partitions.[i]]
                            ]

                        let accWorker = [partitions.[partitions.Length - 1]]
                        return! aux (List.rev accPartitions @ acc) accWorker (accWorkerSize + remainingBytes) workerIndex [] (collectionIndex + 1)
                    else
                        let partitionSize = sizesPerWorker.[workerIndex] - accWorkerSize
                        return! aux acc accWorker 0L (workerIndex + 1) ((workerIndex, partitionSize) :: collectionSlices) collectionIndex

                | [], collection ->
                    // include if remaining capacity is more than half the partition size
                    if (sizesPerWorker.[workerIndex] - accWorkerSize) * 2L > sizes.[collectionIndex] then
                        let partitions = mkPartition workerIndex (collection :: accWorker)
                        return! aux (partitions :: acc) [] 0L (workerIndex + 1) [] (collectionIndex + 1)
                    elif workerIndex = workers.Length - 1 then
                        let remainingPartitions = collections.[collectionIndex ..] |> Array.toList |> List.rev
                        return! aux acc (remainingPartitions @ accWorker) 0L workerIndex [] collections.Length
                    else
                        let partitions = mkPartition workerIndex accWorker
                        return! aux (partitions :: acc) [] 0L (workerIndex + 1)  [] collectionIndex

                | state -> return invalidOp "internal error %A." state
        }

        return! aux [] [] 0L 0 [] 0
    }