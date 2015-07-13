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
    member __.``01. Simple value creation`` () =
        let value = CloudValue.New [1 .. 10000] |> runRemote
        value.Value |> List.length |> shouldEqual 10000

    [<Test>]
    member __.``02. Simple caching`` () =
        cloud {
            let! c = CloudValue.New [1..10000]
            c.IsCachedLocally |> shouldEqual true
            let! v1 = c.GetValueAsync()
            let! v2 = c.GetValueAsync()
            obj.ReferenceEquals(v1,v2) |> shouldEqual true
            return ()
        } |> runRemote

    [<Test>]
    member __.``03. Remote caching`` () =
        cloud {
            let! c = CloudValue.New [1..10000]
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
    member __.``04. Structurally identical values should yield identical cache entities`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let mkValue () = cloud {
                return! CloudValue.New [|for i in 1 .. 1000 -> Some i |]
            }

            let! values = Cloud.Parallel [ for i in 1 .. parallelismFactor -> mkValue () ]
            return values |> Seq.map (fun v -> v.Id) |> Seq.distinct |> Seq.length |> shouldEqual 1
        }

    [<Test>]
    member __.``04. Partitionable CloudValue creation`` () =
        cloud {
            let threshold = 10000L
            let! values = CloudValue.NewPartitioned(seq { 1L .. 10L * threshold }, partitionThreshold = threshold)
            values.Length |> shouldBe (fun l -> l > 5)
        } |> runRemote