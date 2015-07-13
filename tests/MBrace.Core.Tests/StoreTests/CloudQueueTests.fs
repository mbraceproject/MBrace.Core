namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Library

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudQueue Tests`` (parallelismFactor : int) as self =

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
    member __.``Queues: simple send/receive`` () =
        cloud {
            let! cq = CloudQueue.New<int> ()
            let! _,value = CloudQueue.Enqueue(cq, 42) <||> CloudQueue.Dequeue cq
            return value
        } |> runRemote |> shouldEqual 42

    [<Test>]
    member __.``Queues: multiple send/receive`` () =
        cloud {
            let! cq = CloudQueue.New<int option> ()
            let rec sender n = cloud {
                if n = 0 then
                    do! CloudQueue.Enqueue(cq, None)
                else
                    do! CloudQueue.Enqueue(cq, Some n)
                    return! sender (n-1)
            }

            let rec receiver c = cloud {
                let! v = CloudQueue.Dequeue cq
                match v with
                | None -> return c
                | Some i -> return! receiver (c + i)
            }

            let! _, result = sender 100 <||> receiver 0
            return result
        } |> runRemote |> shouldEqual 5050

    [<Test>]
    member __.``Queues: multiple senders`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! cq = CloudQueue.New<int> ()
            let sender n = cloud {
                for i in [1 .. n] do
                    do! CloudQueue.Enqueue(cq, i)
            }

            let rec receiver c n = cloud {
                if n = 0 then return c
                else
                    let! i = CloudQueue.Dequeue cq
                    return! receiver (c + 1) (n - 1)
            }

            let senders = Seq.init parallelismFactor (fun _ -> sender 10) |> Cloud.Parallel |> Cloud.Ignore
            let! _,result = senders <||> receiver 0 (parallelismFactor * 10)
            return result
        } |> runRemote |> shouldEqual (parallelismFactor * 10)
