namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Library

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudQueue Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf 
    let runOnCurrentProcess wf = self.RunLocally wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    /// Specifies if current implementation supports lookup by name
    abstract IsSupportedNamedLookup : bool

    [<Test>]
    member __.``Simple send/receive`` () =
        cloud {
            use! cq = CloudQueue.New<int> ()
            let! _,value = cloud { do! cq.EnqueueAsync 42 } <||> cloud { return! cq.DequeueAsync() }
            return value
        } |> runOnCloud |> shouldEqual 42

    [<Test>]
    member __.``Multiple send/receive`` () =
        cloud {
            use! cq = CloudQueue.New<int option> ()
            let rec sender n = cloud {
                if n = 0 then
                    do! cq.EnqueueAsync None
                else
                    do! cq.EnqueueAsync(Some n)
                    return! sender (n-1)
            }

            let rec receiver c = cloud {
                let! v = cq.DequeueAsync()
                match v with
                | None -> return c
                | Some i -> return! receiver (c + i)
            }

            let! _, result = sender 100 <||> receiver 0
            return result
        } |> runOnCloud |> shouldEqual 5050

    [<Test>]
    member __.``Multiple senders`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            use! cq = CloudQueue.New<int> ()
            let sender n = local {
                for i in 1 .. n do
                    do! cq.EnqueueAsync i
            }

            let rec receiver c n = local {
                if n = 0 then return c
                else
                    let! i = cq.DequeueAsync()
                    return! receiver (c + 1) (n - 1)
            }

            let senders = Seq.init parallelismFactor (fun _ -> sender 10) |> Cloud.Parallel |> Cloud.Ignore
            let! _,result = senders <||> receiver 0 (parallelismFactor * 10)
            return result
        } |> runOnCloud |> shouldEqual (parallelismFactor * 10)

    [<Test>]
    member __.``Batch enqueue/dequeue`` () =
        let xs = [|1 .. 1000|]
        cloud {
            use! queue = CloudQueue.New<int>()
            do! queue.EnqueueBatchAsync xs
            let gathered = new ResizeArray<int>()
            let rec dequeue count = cloud {
                if count <= 0 then return ()
                else
                    let! dq = queue.DequeueBatchAsync(maxItems = count)
                    gathered.AddRange dq
                    return! dequeue (count - dq.Length)
            }
            do! dequeue xs.Length
            return gathered.ToArray()
        } |> runOnCloud |> shouldEqual xs

    [<Test>]
    member __.``Lookup by name`` () =
        if __.IsSupportedNamedLookup then
            cloud {
                use! queue = CloudQueue.New<int> ()
                do! queue.EnqueueAsync 42
                let! queue' = CloudQueue.New<int>(queue.Id)
                let! x = queue'.DequeueAsync()
                return x
            } |> runOnCloud |> shouldEqual 42
