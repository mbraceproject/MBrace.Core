namespace MBrace.Core.Tests

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions

[<TestFixture; AbstractClass>]
type ``CloudValue Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf 

    let mutable counter = 0
    /// avoid caching interference in value creation
    /// by creating unique values on each test run
    let getUniqueValue () =  
        let v = [1 .. 1000000 + counter] 
        counter <- counter + 1
        v
        

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    /// Checks if storage level is supported
    abstract IsSupportedLevel : StorageLevel -> bool

    [<Test>]
    member __.``Simple value creation`` () =
        let value = getUniqueValue()
        let cv = CloudValue.New value |> runOnCloud
        test <@ cv.ReflectedType = typeof<int list> @>
        test <@ cv.Value = value @>

    [<Test>]
    member __.``Null CloudValue`` () =
        let cv1 = CloudValue.New Option<int>.None |> runOnCloud
        let cv2 = CloudValue.New (()) |> runOnCloud
        test <@ cv1.Id = cv2.Id @>
        test <@ cv1.ReflectedType = typeof<obj> @>
        test <@ cv2.ReflectedType = typeof<obj> @>
        let cv = cv1.Cast<obj> ()
        test <@ cv.Value = null @>

    [<Test>]
    member __.``Invalid StorageLevel enumeration passed to CloudValue`` () =
        raises<System.ArgumentException> 
            <@ CloudValue.New(getUniqueValue(), enum 0) |> runOnCloud @>

    [<Test>]
    member __.``CloudValue of array should be CloudArray instance`` () =
        let cv = CloudValue.New [|1 .. 1000|] |> runOnCloud
        let ca = cv :?> CloudArray<int>
        test <@ ca.ReflectedType = typeof<int []> @>
        test <@ ca.Length = 1000 @>

    [<Test>]
    member __.``Simple value memory cached`` () =
        if __.IsSupportedLevel StorageLevel.Memory then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.Memory)
                test <@ c.StorageLevel = StorageLevel.Memory @>
                test <@ c.IsCachedLocally = true @>
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                test <@ obj.ReferenceEquals(v1,v2) = true @>
            } |> runOnCloud

    [<Test>]
    member __.``Simple value memory serialized`` () =
        if __.IsSupportedLevel StorageLevel.MemorySerialized then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.MemorySerialized)
                test <@ c.StorageLevel = StorageLevel.MemorySerialized @>
                test <@ c.IsCachedLocally = true @>
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                test <@ obj.ReferenceEquals(v1,v2) = false @>
            } |> runOnCloud

    [<Test>]
    member __.``Simple value disk persisted`` () =
        if __.IsSupportedLevel StorageLevel.Disk then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.Disk)
                test <@ c.StorageLevel = StorageLevel.Disk @>
                test <@ c.IsCachedLocally = false @>
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                test <@ obj.ReferenceEquals(v1,v2) = false @>
            } |> runOnCloud

    [<Test>]
    member __.``Remote value memory cached`` () =
        if __.IsSupportedLevel StorageLevel.MemoryAndDisk then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.MemoryAndDisk)
                test <@ c.StorageLevel = StorageLevel.MemoryAndDisk @>
                let jobF = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    test <@ obj.ReferenceEquals(v1,v2) = true @>
                    test <@ c.IsCachedLocally = true @>
                }

                let! job = Cloud.CreateProcess jobF
                return! job.AwaitResult()
            } |> runOnCloud

    [<Test>]
    member __.``Remote value memory serialized`` () =
        if __.IsSupportedLevel StorageLevel.MemoryAndDiskSerialized then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.MemoryAndDiskSerialized)
                test <@ c.StorageLevel = StorageLevel.MemoryAndDiskSerialized @>
                let jobF = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    test <@ c.IsCachedLocally = true @>
                    test <@ obj.ReferenceEquals(v1,v2) = false @>
                }

                let! job = Cloud.CreateProcess jobF
                return! job.AwaitResult()

            } |> runOnCloud

    [<Test>]
    member __.``Remote value disk persisted`` () =
        if __.IsSupportedLevel StorageLevel.Disk then
            let value = getUniqueValue()
            cloud {
                use! c = CloudValue.New(value, storageLevel = StorageLevel.Disk)
                test <@ c.StorageLevel = StorageLevel.Disk @>
                let jobF = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    test <@ c.IsCachedLocally = false @>
                    test <@ obj.ReferenceEquals(v1,v2) = false @>
                }

                let! job = Cloud.CreateProcess jobF
                return! job.AwaitResult()
            } |> runOnCloud

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let mkValue () = cloud {
                return! CloudValue.New [|for i in 1 .. 1000 -> Some i |]
            }

            let! values = Cloud.Parallel [ for _ in 1 .. parallelismFactor -> mkValue () ]
            let length = values |> Seq.map (fun v -> v.Id) |> Seq.distinct |> Seq.length 
            test <@ length = 1 @>
        } |> runOnCloud

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities with boxing`` () =
        cloud {
            let value = [|for i in 1 .. 10000 -> sprintf "string %d" i|]
            use! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New (box value)
            test <@ cv2.Id = cv1.Id @>
        } |> runOnCloud

    [<Test>]
    member __.``Casting`` () =
        cloud {
            use! v1 = CloudValue.New [|1 .. 100|]
            let v2 = CloudValue.Cast<obj> v1
            let v3 = CloudValue.Cast<System.Array> v2
            test <@ v2.Id = v1.Id @>
            test <@ v3.Id = v1.Id @>
            test <@ v1.ReflectedType = typeof<int []> @>
            test <@ v2.ReflectedType = typeof<int []> @>
            test <@ v3.ReflectedType = typeof<int []> @>

            raises<System.InvalidCastException> <@ v1.Cast<int> () @>

        } |> runOnCloud

    [<Test>]
    member __.``Simple create new array`` () =
        cloud {
            let size = 100000
            let xs = seq { 
                let r = new System.Random()
                for _ in 1 .. size -> r.Next()
            }

            use! ca = CloudValue.NewArray xs
            test <@ ca.Length = size @>
        } |> runOnCloud

    [<Test>]
    member __.``Partitionable CloudValue creation`` () =
        cloud {
            let threshold = 10000
            let size = 10 * threshold
            let! values = CloudValue.NewArrayPartitioned(seq { 1 .. size }, partitionThreshold = int64 threshold)
            test <@ values.Length >= 10 && values.Length <= 50 @>
            test <@ values |> Seq.sumBy (fun v -> v.Length) = size @>
            test <@ values |> Array.collect (fun vs -> vs.Value) = [|1 .. size|] @>
        } |> runOnCloud

    [<Test>]
    member __.``Random Partitionable CloudValue creation`` () =
        let check (size : int64, threshold : int64) =
            let size = abs size
            let threshold = 1L + abs threshold
            cloud {
                let! values = CloudValue.NewArrayPartitioned(seq { 1L .. size }, partitionThreshold = threshold)
                test <@ values |> Array.sumBy (fun v -> int64 v.Length) = size @>
                test <@ values |> Array.collect (fun vs -> vs.Value) = [|1L .. size|] @>
            } |> runOnCloud

        Check.QuickThrowOnFail(check, maxRuns = 20, shrink = false)

    [<Test>]
    member __.``CloudArray count`` () =
        let level = 
            if __.IsSupportedLevel StorageLevel.Disk then StorageLevel.Disk
            else StorageLevel.Memory

        cloud {
            let size = 10000
            let! value = CloudValue.New<obj>([|1 .. size|], storageLevel = level)
            let value' = value.Cast<int[]>() :?> CloudArray<int>
            let! job = Cloud.CreateProcess(cloud { test <@ value'.Length = size @> })
            return! job.AwaitResult()
        } |> runOnCloud


    [<Test>]
    member __.``Dispose CloudValue`` () =
        let value = getUniqueValue()
        let comp = cloud {
            let! cv = CloudValue.New value
            do! CloudValue.Delete cv
            do! Cloud.Sleep 2000
            let! job = Cloud.CreateProcess(cloud { return! cv.GetValueAsync() })
            return! job.AwaitResult()
        }

        raises<System.ObjectDisposedException> <@ runOnCloud comp @>

    [<Test>]
    member __.``Get CloudValue by Id`` () =
        let value = getUniqueValue()
        cloud {
            use! cv = CloudValue.New value
            let isCacheable = cv.StorageLevel.HasFlag StorageLevel.Memory
            let! job = Cloud.CreateProcess(cloud {
                let! cv' = CloudValue.TryGetValueById cv.Id
                test <@ Option.isSome cv' @>
                let cv' = CloudValue.Cast<int list> cv'.Value
                test <@ cv'.Id = cv.Id @>
                let! v' = cv'.GetValueAsync()
                if isCacheable then test <@ cv'.IsCachedLocally && cv.IsCachedLocally @>
                let! v = cv.GetValueAsync()
                if isCacheable then test <@ obj.ReferenceEquals(v,v') = true @>
                test <@ v' = v @>
            })

            return! job.AwaitResult()
        } |> runOnCloud

    [<Test>]
    member __.``Get Disposed CloudValue by Id`` () =
        let value = getUniqueValue()
        let comp = cloud {
            let! cv = CloudValue.New value
            let id = cv.Id
            do! CloudValue.Delete cv
            do! Cloud.Sleep 2000
            let! job = Cloud.CreateProcess(cloud {
                return! CloudValue.TryGetValueById id
            })
            return! job.AwaitResult()

        } 

        test <@ runOnCloud comp = None @>

    [<Test>]
    member __.``Get All CloudValues`` () =
        let value = getUniqueValue()
        let value' = getUniqueValue()
        cloud {
            let! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New value'
            let! job = Cloud.CreateProcess(cloud {
                let! allValues = CloudValue.GetAllValues()
                test <@ allValues |> Array.exists (fun v -> v.Id = cv1.Id && cv1.Value = (v.Cast<int list>()).Value) @>
                test <@ allValues |> Array.exists (fun v -> v.Id = cv2.Id && cv2.Value = (v.Cast<int list>()).Value) @>
            })
            return! job.AwaitResult()
        } |> runOnCloud