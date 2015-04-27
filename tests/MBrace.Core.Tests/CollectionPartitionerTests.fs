namespace MBrace.Core.Tests

open System
open System.Threading

open FsCheck

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Workflows
open MBrace.Client

[<TestFixture>]
[<Category("CollectionPartitioner")>]
module ``Collection Partitioning Tests`` =

    let imem = LocalRuntime.Create(ResourceRegistry.Empty)
    let run c = imem.Run c

    let mkDummyWorker id cores =
        { 
           new obj() with
              member x.Equals(obj) =
                match obj with :? IWorkerRef as w -> id = w.Id | _ -> false
              member x.ToString() = sprintf "worker%s" id

           interface IWorkerRef with
              member x.CompareTo(obj: obj): int = 
                match obj with :? IWorkerRef as w -> compare id w.Id | _ -> invalidArg "obj" "invalid comparand."
              member x.Id: string = id
              member x.ProcessorCount: int = cores
              member x.Type: string = "dummy"
        }

    let concat (collections : seq<#ICloudCollection<'T>>) =
        { new ICloudCollection<'T> with
              member x.Count: Local<int64> = local { let! cs = collections |> Sequential.map (fun c -> c.Count) in return Array.sum cs}
              member x.Size: Local<int64> = local { let! cs = collections |> Sequential.map (fun c -> c.Size) in return Array.sum cs}
              member x.IsKnownCount: bool = collections |> Seq.forall (fun c -> c.IsKnownCount)
              member x.IsKnownSize: bool = collections |> Seq.forall (fun c -> c.IsKnownSize)
              member x.ToEnumerable(): Local<seq<'T>> = local { let! cs = collections |> Sequential.map (fun c -> c.ToEnumerable()) in return Seq.concat cs }
        }

    type RangeCollection(lower : int64, upper : int64, discloseSize : bool) =
        static member Empty(discloseSize) = new RangeCollection(0L, -1L, discloseSize)
        interface ICloudCollection<int64> with
            member x.Count: Local<int64> = local { return max 0L (upper - lower) }
            member x.IsKnownCount: bool = discloseSize
            member x.IsKnownSize: bool = discloseSize
            member x.Size: Local<int64> = local { return max 0L (upper - lower) }
            member x.ToEnumerable(): Local<seq<int64>> = local { return seq { lower .. upper - 1L } }

        override x.Equals y = obj.ReferenceEquals(x,y)
        override x.GetHashCode() = (lower,upper).GetHashCode()

    type PartitionableRangeCollection(lower : int64, upper : int64) =
        inherit RangeCollection(lower, upper, true)
        interface IPartitionableCollection<int64> with
            member x.GetPartitions(weights: int []): Local<ICloudCollection<int64> []> = local {
                return
                    Array.splitWeightedRange weights lower upper
                    |> Array.map (function Some(l,u) -> new RangeCollection(l,u + 1L,true) :> ICloudCollection<int64>
                                            | None -> RangeCollection.Empty(true) :> _)
            }

    [<Test>]
    let ``Range collection tests`` () =
        let tester (lower : int64, length : uint32) =
            let length = int64 length
            let upper = lower + length
            let range = new RangeCollection(lower, upper, true) :> ICloudCollection<int64>
            range.IsKnownSize |> shouldEqual true
            range.Count |> run |> shouldEqual length
            range.ToEnumerable() |> run |> Seq.length |> int64 |> shouldEqual length

        Check.QuickThrowOnFail(tester, maxRuns = 500)

    [<Test>]
    let ``Partitionable Range collection tests`` () =
        let tester (lower : int64, length : uint32, weights : uint16 []) =
            if weights = null || weights.Length = 0 then () else
            let length = int64 length
            let upper = lower + length
            let weights = weights |> Array.map (fun i -> int i + 1)
            let range = new PartitionableRangeCollection(lower, upper) :> IPartitionableCollection<int64>
            let partitions = range.GetPartitions weights |> run
            let partitionedSeqs = partitions |> Sequential.map (fun p -> p.ToEnumerable()) |> run |> Seq.concat
            partitionedSeqs |> Seq.length |> int64 |> shouldEqual length

        Check.QuickThrowOnFail(tester, maxRuns = 500)

    [<Test>]
    let ``Partition collections that do not disclose size`` () =
        let tester (isTargeted : bool, partitions : uint16, workers : uint16) =
            let partitions = [| for p in 0us .. partitions -> RangeCollection.Empty(discloseSize = false) :> ICloudCollection<int64> |]
            let workers = [| for w in 0us .. workers -> mkDummyWorker (string w) 4 |]
            let partitionss = CloudCollection.PartitionBySize(workers, isTargeted, partitions) |> run
            partitionss |> Array.map fst |> shouldEqual workers
            partitionss |> Array.collect snd |> shouldEqual partitions

        Check.QuickThrowOnFail(tester, maxRuns = 500)

    [<Test>]
    let ``Partition collections that disclose size`` () =
        let tester (isTargeted : bool, sizes : uint32 [], workers : uint16) =
            let partitions = [| for size in sizes -> new RangeCollection(0L, int64 size, discloseSize = true) :> ICloudCollection<int64> |]
            let workers = [| for w in 0us .. workers -> mkDummyWorker (string w) 4 |]
            let partitionss = CloudCollection.PartitionBySize(workers, isTargeted, partitions) |> run
            partitionss |> Array.map fst |> shouldEqual workers
            partitionss |> Array.collect snd |> shouldEqual partitions

        Check.QuickThrowOnFail(tester, maxRuns = 500)

    [<Test>]
    let ``Partitionable collection simple`` () =
        let tester (isTargeted : bool, totalSize : int64, workerCores : uint16 []) =
            if workerCores.Length = 0 then () else
            let totalSize = abs totalSize
            let partitionable = new PartitionableRangeCollection(0L, totalSize)
            let workers = [| for i in 0 .. workerCores.Length - 1 -> mkDummyWorker (string i) (1 + int workerCores.[i]) |]
            let partitionss = CloudCollection.PartitionBySize(workers, isTargeted, [|partitionable|]) |> run
            let sizes = partitionss |> Seq.collect snd |> Sequential.map (fun c -> c.Size) |> run
            Array.sum sizes |> shouldEqual totalSize

        Check.QuickThrowOnFail(tester, maxRuns = 500)

    [<Test>]
    let ``Partitionable collections combined`` () =
        let tester (isTargeted : bool, totalSizes : int64 [], workerCores : uint16 []) =
            if workerCores.Length = 0 then () else
            let partitionables = totalSizes |> Array.map (fun s -> new PartitionableRangeCollection(0L, abs s) :> ICloudCollection<int64>)
            let workers = [| for i in 0 .. workerCores.Length - 1 -> mkDummyWorker (string i) (1 + int workerCores.[i]) |]
            let partitionss = CloudCollection.PartitionBySize(workers, isTargeted, partitionables) |> run
            let sizes = partitionss |> Seq.collect snd |> Sequential.map (fun c -> c.Size) |> run
            Array.sum sizes |> shouldEqual (totalSizes |> Array.sumBy (fun s -> abs s))

        Check.QuickThrowOnFail(tester, maxRuns = 500)