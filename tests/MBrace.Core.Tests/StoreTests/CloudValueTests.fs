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

    /// Run workflow in the runtime under test
    abstract RunRemote : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T

    [<Test>]
    member __.``Simple value creation`` () =
        let value = CloudValue.New [1 .. 10000] |> runRemote
        value.Value |> List.length |> shouldEqual 10000

    [<Test>]
    member __.``Simple value memory cached`` () =
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.Memory)
            if c.StorageLevel = StorageLevel.Memory then
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual true
        } |> runRemote

    [<Test>]
    member __.``Simple value memory serialized`` () =
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.MemorySerialized)
            if c.StorageLevel = StorageLevel.MemorySerialized then
                c.IsCachedLocally |> shouldEqual true
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
        } |> runRemote

    [<Test>]
    member __.``Simple value disk cached`` () =
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.Disk)
            if c.StorageLevel = StorageLevel.Disk then
                c.IsCachedLocally |> shouldEqual false
                let! v1 = c.GetValueAsync()
                let! v2 = c.GetValueAsync()
                obj.ReferenceEquals(v1,v2) |> shouldEqual false
        } |> runRemote

    [<Test>]
    member __.``Remote value memory cached`` () =
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.Memory)
            if c.StorageLevel = StorageLevel.Memory then
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
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.MemorySerialized)
            if c.StorageLevel = StorageLevel.MemorySerialized then
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
        cloud {
            let! c = CloudValue.New([1..10000], storageLevel = StorageLevel.Disk)
            if c.StorageLevel = StorageLevel.Disk then
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
        let parallelismFactor = parallelismFactor
        cloud {
            let value = [|for i in 1 .. 1000000 -> sprintf "string %d" i|]
            let! cv1 = CloudValue.New value
            let! cv2 = CloudValue.New (box value)
            cv2.Id |> shouldEqual cv1.Id
        } |> runRemote

    [<Test>]
    member __.``Partitionable CloudValue creation`` () =
        cloud {
            let threshold = 10000
            let size = 10 * threshold
            let! values = CloudValue.NewPartitioned(seq { 1 .. 10 * threshold }, partitionThreshold = int64 threshold)
            values.Length |> shouldBe (fun l -> l > size / 2)
            values |> Seq.concat |> Seq.length |> shouldEqual size
        } |> runRemote