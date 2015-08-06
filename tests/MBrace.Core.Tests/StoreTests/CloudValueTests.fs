namespace MBrace.Core.Tests

open NUnit.Framework

open MBrace.Core

[<TestFixture; AbstractClass>]
type ``CloudValue Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.RunOnCloud wf 
    let runOnCurrentMachine wf = self.RunOnCurrentMachine wf

    let runProtected wf = 
        try self.RunOnCloud wf |> Choice1Of2
        with e -> Choice2Of2 e

    let mutable counter = 0
    /// avoid caching interference in value creation
    /// by creating unique values on each test run
    let getUniqueValue () =  
        let v = [1 .. 1000000 + counter] 
        counter <- counter + 1
        v
        

    /// Run workflow in the runtime under test
    abstract RunOnCloud : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunOnCurrentMachine : Cloud<'T> -> 'T
    /// Checks if storage level is supported
    abstract IsSupportedLevel : StorageLevel -> bool

    [<Test>]
    member __.``Simple value creation`` () =
        let value = getUniqueValue()
        let cv = CloudValue.New value |> runOnCloud
        cv.ReflectedType |> shouldEqual typeof<int list>
        cv.Value |> shouldEqual value

    [<Test>]
    member __.``Null CloudValue`` () =
        let cv1 = CloudValue.New Option<int>.None |> runOnCloud
        let cv2 = CloudValue.New (()) |> runOnCloud
        cv1.Id |> shouldEqual cv2.Id
        cv1.ReflectedType |> shouldEqual typeof<obj>
        cv2.ReflectedType |> shouldEqual typeof<obj>
        let cv = cv1.Cast<obj> ()
        cv.Value |> shouldEqual null

    [<Test>]
    member __.``Invalid StorageLevel enumeration passed to CloudValue`` () =
        fun () -> CloudValue.New(getUniqueValue(), enum 0) |> runOnCloud
        |> shouldFailwith<_, System.ArgumentException>

    [<Test>]
    member __.``CloudValue of array should be CloudArray instance`` () =
        let cv = CloudValue.New [|1 .. 1000|] |> runOnCloud
        let ca = cv :?> CloudArray<int>
        ca.ReflectedType |> shouldEqual typeof<int []>
        ca.Length |> shouldEqual 1000

    [<Test>]
    member __.``Simple value memory cached`` () =
        if __.IsSupportedLevel StorageLevel.Memory then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.Memory)
                c.StorageLevel |> shouldEqual StorageLevel.Memory
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual true
            } |> runOnCloud

    [<Test>]
    member __.``Simple value memory serialized`` () =
        if __.IsSupportedLevel StorageLevel.MemorySerialized then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.MemorySerialized)
                c.StorageLevel |> shouldEqual StorageLevel.MemorySerialized
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
            } |> runOnCloud

    [<Test>]
    member __.``Simple value disk persisted`` () =
        if __.IsSupportedLevel StorageLevel.Disk then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.Disk)
                c.StorageLevel |> shouldEqual StorageLevel.Disk
                c.IsCachedLocally |> shouldEqual false
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
            } |> runOnCloud

    [<Test>]
    member __.``Remote value memory cached`` () =
        if __.IsSupportedLevel StorageLevel.MemoryAndDisk then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.MemoryAndDisk)
                c.StorageLevel |> shouldEqual StorageLevel.MemoryAndDisk
                let taskF () = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    obj.ReferenceEquals(v1,v2) |> shouldEqual true
                    c.IsCachedLocally |> shouldEqual true
                }

                let! task = Cloud.StartAsTask(taskF())
                return! task.AwaitResult()
            } |> runOnCloud

    [<Test>]
    member __.``Remote value memory serialized`` () =
        if __.IsSupportedLevel StorageLevel.MemoryAndDiskSerialized then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.MemoryAndDiskSerialized)
                c.StorageLevel |> shouldEqual StorageLevel.MemoryAndDiskSerialized
                let taskF () = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    c.IsCachedLocally |> shouldEqual true
                    obj.ReferenceEquals(v1,v2) |> shouldEqual false
                }

                let! task = Cloud.StartAsTask(taskF())
                return! task.AwaitResult()

            } |> runOnCloud

    [<Test>]
    member __.``Remote value disk persisted`` () =
        if __.IsSupportedLevel StorageLevel.Disk then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.Disk)
                c.StorageLevel |> shouldEqual StorageLevel.Disk
                let taskF () = cloud {
                    let! v1 = c.GetValueAsync()
                    let! v2 = c.GetValueAsync()
                    c.IsCachedLocally |> shouldEqual false
                    obj.ReferenceEquals(v1,v2) |> shouldEqual false
                }

                let! task = Cloud.StartAsTask(taskF())
                return! task.AwaitResult()
            } |> runOnCloud

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let mkValue () = cloud {
                return! CloudValue.New [|for i in 1 .. 1000 -> Some i |]
            }

            let! values = Cloud.Parallel [ for i in 1 .. parallelismFactor -> mkValue () ]
            return values |> Seq.map (fun v -> v.Id) |> Seq.distinct |> Seq.length |> shouldEqual 1
        } |> runOnCloud

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities with boxing`` () =
        cloud {
            let value = [|for i in 1 .. 10000 -> sprintf "string %d" i|]
            let! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New (box value)
            cv2.Id |> shouldEqual cv1.Id
        } |> runOnCloud

    [<Test>]
    member __.``Casting`` () =
        cloud {
            let! v1 = CloudValue.New [|1 .. 100|]
            let v2 = CloudValue.Cast<obj> v1
            let v3 = CloudValue.Cast<System.Array> v2
            v2.Id |> shouldEqual v1.Id
            v3.Id |> shouldEqual v1.Id
            v1.ReflectedType |> shouldEqual typeof<int []>
            v2.ReflectedType |> shouldEqual typeof<int []>
            v3.ReflectedType |> shouldEqual typeof<int []>

            fun () -> v1.Cast<int> ()
            |> shouldFailwith<_, System.InvalidCastException>

        } |> runOnCloud

    [<Test>]
    member __.``Simple create new array`` () =
        cloud {
            let size = 100000
            let xs = seq { 
                let r = new System.Random()
                for i in 1 .. size -> r.Next()
            }

            let! ca = CloudValue.NewArray xs
            ca.Length |> shouldEqual size
        } |> runOnCloud

    [<Test>]
    member __.``Partitionable CloudValue creation`` () =
        cloud {
            let threshold = 10000
            let size = 10 * threshold
            let! values = CloudValue.NewArrayPartitioned(seq { 1 .. size }, partitionThreshold = int64 threshold)
            values.Length |> shouldBe (fun l -> l >= 10 && l <= 50)
            values |> Seq.sumBy (fun v -> v.Length) |> shouldEqual size
            values |> Array.collect (fun vs -> vs.Value) |> shouldEqual [|1 .. size|]
        } |> runOnCloud

    [<Test>]
    member __.``Random Partitionable CloudValue creation`` () =
        let check (size : int64, threshold : int64) =
            let size = abs size
            let threshold = 1L + abs threshold
            cloud {
                let! values = CloudValue.NewArrayPartitioned(seq { 1L .. size }, partitionThreshold = threshold)
                values |> Array.sumBy (fun v -> int64 v.Length) |> shouldEqual size
                values |> Array.collect (fun vs -> vs.Value) |> shouldEqual [|1L .. size|]
            } |> runOnCloud

        Check.QuickThrowOnFail(check, maxRuns = 20)

    [<Test>]
    member __.``CloudArray count`` () =
        let level = 
            if __.IsSupportedLevel StorageLevel.Disk then StorageLevel.Disk
            else StorageLevel.Memory

        cloud {
            let size = 10000
            let! value = CloudValue.New<obj>([|1 .. size|], storageLevel = level)
            let value' = value.Cast<int[]>() :?> CloudArray<int>
            let! t = Cloud.StartAsTask(cloud { value'.Length |> shouldEqual size })
            return! t.AwaitResult()
        } |> runOnCloud


    [<Test>]
    member __.``Dispose CloudValue`` () =
        let value = getUniqueValue()
        fun () ->
            cloud {
                let! cv = CloudValue.New value
                do! CloudValue.Delete cv
                do! Cloud.Sleep 2000
                let! t = Cloud.StartAsTask(cloud { return! cv.GetValueAsync() })
                return! t.AwaitResult()
            } |> runOnCloud

        |> shouldFailwith<_,System.ObjectDisposedException>

    [<Test>]
    member __.``Get CloudValue by Id`` () =
        let value = getUniqueValue()
        cloud {
            let! cv = CloudValue.New value
            let isCacheable = cv.StorageLevel.HasFlag StorageLevel.Memory
            let! t = Cloud.StartAsTask(cloud {
                let! cv' = CloudValue.TryGetValueById cv.Id
                let cv' = CloudValue.Cast<int list> cv
                cv'.Id |> shouldEqual cv.Id
                let! v' = cv'.GetValueAsync()
                if isCacheable then cv'.IsCachedLocally |> shouldEqual true
                if isCacheable then cv.IsCachedLocally |> shouldEqual true
                let! v = cv.GetValueAsync()
                if isCacheable then obj.ReferenceEquals(v,v') |> shouldEqual true
                v' |> shouldEqual v
            })

            return! t.AwaitResult()
        } |> runOnCloud

    [<Test>]
    member __.``Get Disposed CloudValue by Id`` () =
        let value = getUniqueValue()
        cloud {
            let! cv = CloudValue.New value
            let id = cv.Id
            do! CloudValue.Delete cv
            do! Cloud.Sleep 2000
            let! t = Cloud.StartAsTask(cloud {
                return! CloudValue.TryGetValueById id
            })
            return! t.AwaitResult()

        } |> runOnCloud |> shouldEqual None

    [<Test>]
    member __.``Get All CloudValues`` () =
        let value = getUniqueValue()
        let value' = getUniqueValue()
        cloud {
            let! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New value'
            let! t = Cloud.StartAsTask(cloud {
                let! allValues = CloudValue.GetAllValues()
                allValues |> Array.exists (fun v -> v.Id = cv1.Id && cv1.Value = (v.Cast<int list>()).Value) |> shouldEqual true
                allValues |> Array.exists (fun v -> v.Id = cv2.Id && cv2.Value = (v.Cast<int list>()).Value) |> shouldEqual true
            })
            return! t.AwaitResult()
        } |> runOnCloud
