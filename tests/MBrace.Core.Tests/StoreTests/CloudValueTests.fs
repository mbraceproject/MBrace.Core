namespace MBrace.Core.Tests

open NUnit.Framework

open MBrace.Core

[<TestFixture; AbstractClass>]
type ``CloudValue Tests`` (parallelismFactor : int) as self =

    let runRemote wf = self.RunRemote wf 
    let runLocally wf = self.RunLocally wf

    let runProtected wf = 
        try self.RunRemote wf |> Choice1Of2
        with e -> Choice2Of2 e

    let mutable counter = 0
    /// avoid caching interference in value creation
    /// by creating unique values on each test run
    let getUniqueValue () =  
        let v = [1 .. 10000 + counter] 
        counter <- counter + 1
        v
        

    /// Run workflow in the runtime under test
    abstract RunRemote : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    abstract IsMemorySupported : bool
    abstract IsMemorySerializedSupported : bool
    abstract IsDiskSupported : bool

    [<Test>]
    member __.``Simple value creation`` () =
        let value = CloudValue.New [1 .. 10000] |> runRemote
        value.Value |> List.length |> shouldEqual 10000

    [<Test>]
    member __.``CloudValue of array should be CloudArray instance`` () =
        let cv = CloudValue.New [|1 .. 1000|] |> runRemote
        let ca = cv :?> ICloudArray<int>
        ca.Length |> shouldEqual 1000

    [<Test>]
    member __.``Simple value memory cached`` () =
        if __.IsMemorySupported then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.Memory)
                c.StorageLevel |> shouldEqual StorageLevel.Memory
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual true
            } |> runRemote

    [<Test>]
    member __.``Simple value memory serialized`` () =
        if __.IsMemorySerializedSupported then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.MemorySerialized)
                c.StorageLevel |> shouldEqual StorageLevel.MemorySerialized
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
            } |> runRemote

    [<Test>]
    member __.``Simple value disk cached`` () =
        if __.IsDiskSupported then
            let value = getUniqueValue()
            cloud {
                let! c = CloudValue.New(value, storageLevel = StorageLevel.Disk)
                c.StorageLevel |> shouldEqual StorageLevel.Disk
                c.IsCachedLocally |> shouldEqual false
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
            } |> runRemote

    [<Test>]
    member __.``Remote value memory cached`` () =
        if __.IsMemorySupported && __.IsDiskSupported then
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
            } |> runRemote

    [<Test>]
    member __.``Remote value memory serialized`` () =
        if __.IsMemorySerializedSupported && __.IsDiskSupported then
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

            } |> runRemote

    [<Test>]
    member __.``Remote value disk cached`` () =
        if __.IsDiskSupported then
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
            } |> runRemote

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let mkValue () = cloud {
                return! CloudValue.New [|for i in 1 .. 1000 -> Some i |]
            }

            let! values = Cloud.Parallel [ for i in 1 .. parallelismFactor -> mkValue () ]
            return values |> Seq.map (fun v -> v.Id) |> Seq.distinct |> Seq.length |> shouldEqual 1
        } |> runRemote

    [<Test>]
    member __.``Structurally identical values should yield identical cache entities with boxing`` () =
        cloud {
            let value = [|for i in 1 .. 10000 -> sprintf "string %d" i|]
            let! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New (box value)
            cv2.Id |> shouldEqual cv1.Id
        } |> runRemote

    [<Test>]
    member __.``Casting`` () =
        cloud {
            let! v1 = CloudValue.New [|1 .. 100|]
            let v2 = CloudValue.Cast<obj> v1
            let v3 = CloudValue.Cast<System.Array> v2
            v2.Id |> shouldEqual v1.Id
            v3.Id |> shouldEqual v1.Id

            fun () -> v1.Cast<int> ()
            |> shouldFailwith<_, System.InvalidCastException>

        } |> runRemote

    [<Test>]
    member __.``Partitionable CloudValue creation`` () =
        cloud {
            let threshold = 10000
            let size = 10 * threshold
            let! values = CloudValue.NewPartitioned(seq { 1 .. size }, partitionThreshold = int64 threshold)
            values.Length |> shouldBe (fun l -> l >= 10 && l <= 50)
            values |> Seq.concat |> Seq.length |> shouldEqual size
        } |> runRemote