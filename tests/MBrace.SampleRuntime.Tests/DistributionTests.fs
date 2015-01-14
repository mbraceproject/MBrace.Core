namespace MBrace.SampleRuntime.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework
open FsUnit

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Tests
open MBrace.SampleRuntime
open MBrace.SampleRuntime.Actors

[<TestFixture>]
module ``SampleRuntime Tests`` =
    
    [<Literal>]
#if DEBUG
    let repeats = 5
#else
    let repeats = 1
#endif

    let mutable runtime : MBraceRuntime option = None

    type System.Threading.Tasks.Task<'T> with
        member t.CorrectResult =
            try t.Result
            with :? AggregateException as e -> 
                raise e.InnerException


    [<TestFixtureSetUp>]
    let init () =
        MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"
        runtime <- Some <| MBraceRuntime.InitLocal(4)

    [<TestFixtureTearDown>]
    let fini () =
        runtime |> Option.iter (fun r -> r.KillAllWorkers())
        runtime <- None

    type Latch with
        member l.Incr() = l.Increment() |> Async.RunSync

    let run (workflow : Cloud<'T>) = Option.get(runtime).RunAsync workflow |> Async.Catch |> Async.RunSynchronously
    let runCts (workflow : DistributedCancellationTokenSource -> Cloud<'T>) = 
        async {
            let runtime = Option.get runtime
            let dcts = DistributedCancellationTokenSource.Init()
            let ct = dcts.GetLocalCancellationToken()
            return! runtime.RunAsync(workflow dcts, cancellationToken = ct) |> Async.Catch
        } |> Async.RunSync

    [<Test>]
    let ``1. Parallel : empty input`` () =
        run (Cloud.Parallel List.empty<Cloud<int>>) |> Choice.shouldEqual [||]

    [<Test>]
    let ``1. Parallel : simple inputs`` () =
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual 210

    [<Test>]
    let ``1. Parallel : use binding`` () =
        let latch = Latch.Init 0
        cloud {
            use foo = { new ICloudDisposable with member __.Dispose () = async { return latch.Incr() |> ignore } }
            let! _ = cloud { return latch.Incr() } <||> cloud { return latch.Incr() }
            return latch.Value
        } |> run |> Choice.shouldEqual 2

        latch.Value |> should equal 3

    [<Test>]
    let ``1. Parallel : exception handler`` () =
        cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            with :? InvalidOperationException as e ->
                let! x,y = cloud { return 1 } <||> cloud { return 2 }
                return x + y
        } |> run |> Choice.shouldEqual 3

    [<Test>]
    let ``1. Parallel : finally`` () =
        let latch = Latch.Init 0
        cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            finally
                latch.Incr () |> ignore
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        latch.Value |> should equal 1

    [<Test>]
    let ``1. Parallel : simple nested`` () =
        cloud {
            let f i j = cloud { return i + j + 2 }
            let cluster i = Array.init 10 (f i) |> Cloud.Parallel
            let! results = Array.init 10 cluster |> Cloud.Parallel
            return Array.concat results |> Array.sum
        } |> run |> Choice.shouldEqual 1100

    [<Test>]
    let ``1. Parallel : simple exception`` () =
        cloud {
            let f i = cloud { return if i = 15 then invalidOp "failure" else i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : exception contention`` () =
        let latch = Latch.Init(0)
        cloud {
            try
                let! _ = Array.init 20 (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                latch.Incr() |> ignore
                return ()
        } |> run |> Choice.shouldEqual ()

        // test that exception continuation was fired precisely once
        latch.Value |> should equal 1


    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : exception cancellation`` () =
        cloud {
            let latch = Latch.Init 0
            let worker i = cloud { 
                if i = 0 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = latch.Incr()
                    return ()
            }

            try
                let! _ = Array.init 20 worker |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                return latch.Value
        } |> run |> Choice.shouldMatch(fun i -> i < 5)

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : nested exception cancellation`` () =
        cloud {
            let latch = Latch.Init 0
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = latch.Incr()
                    return ()
            }

            try
                let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
                do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                return latch.Value
        } |> run |> Choice.shouldMatch(fun i -> i < 50)


    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : simple cancellation`` () =
        let latch = Latch.Init 0
        runCts(fun cts -> cloud {
            let f i = cloud {
                if i = 0 then cts.Cancel() 
                do! Cloud.Sleep 3000 
                return latch.Incr() 
            }

            let! _ = Array.init 10 f |> Cloud.Parallel

            return ()
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        latch.Value |> should equal 0


    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : to local`` () =
        // check local semantics are forced by using ref cells.
        cloud {
            let counter = ref 0
            let seqWorker _ = cloud {
                do! Cloud.Sleep 10
                Interlocked.Increment counter |> ignore
            }

            let! results = Array.init 20 seqWorker |> Cloud.Parallel |> Cloud.ToLocal
            return counter.Value
        } |> run |> Choice.shouldEqual 20

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : to sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker _ = cloud {
                let init = counter.Value + 1
                counter := init
                do! Cloud.Sleep 10
                return counter.Value = init
            }

            let! results = Array.init 20 seqWorker |> Cloud.Parallel |> Cloud.ToSequential
            return Array.forall id results
        } |> run |> Choice.shouldEqual true

    let wordCount size mapReduceAlgorithm : Cloud<int> =
        let mapF (text : string) = cloud { return text.Split(' ').Length }
        let reduceF i i' = cloud { return i + i' }
        let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
        mapReduceAlgorithm mapF 0 reduceF inputs

    let rec mapReduceRec (mapF : 'T -> Cloud<'S>) 
                    (id : 'S) (reduceF : 'S -> 'S -> Cloud<'S>)
                    (inputs : 'T []) =
        cloud {
            match inputs with
            | [||] -> return id
            | [|t|] -> return! mapF t
            | _ ->
                let left = inputs.[.. inputs.Length / 2 - 1]
                let right = inputs.[inputs.Length / 2 ..]
                let! s,s' = (mapReduceRec mapF id reduceF left) <||> (mapReduceRec mapF id reduceF right)
                return! reduceF s s'
        }

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : recursive map/reduce`` () =
        wordCount 20 mapReduceRec |> run |> Choice.shouldEqual 100

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : balanced map/reduce`` () =
        wordCount 1000 MapReduce.mapReduce |> run |> Choice.shouldEqual 5000

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : to all workers`` () =
        cloud {
            let! workers = Cloud.GetAvailableWorkers()
            let! results = Cloud.Parallel Cloud.CurrentWorker
            return set results = set workers
        } |> run |> Choice.shouldEqual true

    [<Test>]
    [<Repeat(repeats)>]
    let ``1. Parallel : to current worker`` () =
        cloud {
            let! thisWorker = Cloud.CurrentWorker
            let! results = Cloud.Parallel [(Cloud.CurrentWorker, thisWorker)]
            return results.[0] = thisWorker
        } |> run |> Choice.shouldEqual true

    [<Test>]
    let ``1. Parallel : to all workers in local semantics`` () =
        Cloud.CurrentWorker
        |> Cloud.Parallel
        |> Cloud.ToLocal
        |> run
        |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    let ``2. Choice : empty input`` () =
        Cloud.Choice List.empty<Cloud<int option>> |> run |> Choice.shouldEqual None

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : all inputs 'None'`` () =
        cloud {
            let count = Latch.Init 0
            let worker _ = cloud {
                let _ = count.Incr()
                return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return (count.Value, result)
        } |> run |> Choice.shouldEqual (20, None)


    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : one input 'Some'`` () =
        cloud {
            let count = Latch.Init 0
            let worker i = cloud {
                if i = 0 then return Some i
                else
                    do! Cloud.Sleep 1000
                    // check proper cancellation while we're at it.
                    let _ = count.Incr()
                    return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return result, count.Value
        } |> run |> Choice.shouldEqual (Some 0, 0)

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : all inputs 'Some'`` () =
        let successcounter = Latch.Init 0
        cloud {
            let worker _ = cloud { return Some 42 }
            let! result = Array.init 20 worker |> Cloud.Choice
            let _ = successcounter.Incr()
            return result
        } |> run |> Choice.shouldEqual (Some 42)

        // ensure only one success continuation call
        successcounter.Value |> should equal 1

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : simple nested`` () =
        let counter = Latch.Init 0
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    return Some(i,j)
                else
                    do! Cloud.Sleep 100
                    let _ = counter.Incr()
                    return None
            }

            let cluster i = Array.init 4 (worker i) |> Cloud.Choice
            let! result = Array.init 5 cluster |> Cloud.Choice
            return result
        } |> run |> Choice.shouldEqual (Some (0,0))

        counter.Value |> should be (lessThan 30)

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : nested exception cancellation`` () =
        let counter = Latch.Init 0
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    return invalidOp "failure"
                else
                    do! Cloud.Sleep 3000
                    let _ = counter.Incr()
                    return Some 42
            }

            let cluster i = Array.init 5 (worker i) |> Cloud.Choice
            return! Array.init 4 cluster |> Cloud.Choice
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        counter.Value |> should be (lessThan 30)

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : simple cancellation`` () =
        let taskCount = Latch.Init 0
        runCts(fun cts ->
            cloud {
                let worker i = cloud {
                    if i = 0 then cts.Cancel()
                    do! Cloud.Sleep 3000
                    let _ = taskCount.Incr()
                    return Some 42
                }

                return! Array.init 10 worker |> Cloud.Choice
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        taskCount.Value |> should equal 0

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : to local`` () =
        // check local semantics are forced by using ref cells.
        cloud {
            let counter = ref 0
            let seqWorker i = cloud {
                if i = 16 then
                    do! Cloud.Sleep 100
                    return Some i
                else
                    let _ = Interlocked.Increment counter
                    return None
            }

            let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToLocal
            return result, counter.Value
        } |> run |> Choice.shouldEqual (Some 16, 19)

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : to sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker i = cloud {
                let init = counter.Value + 1
                counter := init
                do! Cloud.Sleep 10
                counter.Value |> should equal init
                if i = 16 then
                    return Some ()
                else
                    return None
            }

            let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToSequential
            return result, counter.Value
        } |> run |> Choice.shouldEqual (Some(), 17)

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : to all workers`` () =
        cloud {
            let! workers = Cloud.GetAvailableWorkers()
            let latch = Latch.Init 0
            let! _ = Cloud.Choice (cloud { let _ = latch.Incr() in return Option<int>.None })
            return latch.Value = workers.Length
        } |> run |> Choice.shouldEqual true

    [<Test>]
    [<Repeat(repeats)>]
    let ``2. Choice : to current worker`` () =
        cloud {
            let! thisWorker = Cloud.CurrentWorker
            let! results = Cloud.Choice [(cloud { let! w = Cloud.CurrentWorker in return Some w }, thisWorker)]
            return results.Value = thisWorker
        } |> run |> Choice.shouldEqual true

    [<Test>]
    let ``2. Choice : to all workers in local semantics`` () =
        cloud { return Some 42 }
        |> Cloud.Choice
        |> Cloud.ToLocal
        |> run
        |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(repeats)>]
    let ``3. StartChild: task with success`` () =
        cloud {
            let count = Latch.Init 0
            let task = cloud {
                do! Cloud.Sleep 100
                return count.Incr()
            }

            let! ch = Cloud.StartChild(task)
            count.Value |> should equal 0
            return! ch
        } |> run |> Choice.shouldEqual 1

    [<Test>]
    [<Repeat(repeats)>]
    let ``3. StartChild: task with exception`` () =
        let count = Latch.Init 0
        cloud {
            let task = cloud {
                do! Cloud.Sleep 100
                let _ = count.Incr()
                return invalidOp "failure"
            }

            let! ch = Cloud.StartChild(task)
            count.Value |> should equal 0
            do! Cloud.Sleep 100
            // ensure no exception is raised in parent workflow
            // before the child workflow is properly evaluated
            let _ = count.Incr()
            return! ch
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        count.Value |> should equal 2

    [<Test>]
    [<Repeat(repeats)>]
    let ``3. StartChild: task with cancellation`` () =
        let count = Latch.Init 0
        runCts(fun cts ->
            cloud {
                let task = cloud {
                    let _ = count.Incr()
                    do! Cloud.Sleep 3000
                    return count.Incr()
                }

                let! ch = Cloud.StartChild(task)
                do! Cloud.Sleep 1000
                count.Value |> should equal 1
                cts.Cancel ()
                return! ch
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        // ensure final increment was cancelled.
        count.Value |> should equal 1

    [<Test>]
    [<Repeat(repeats)>]
    let ``3. StartChild: to current worker`` () =
        cloud {
            let! currentWorker = Cloud.CurrentWorker
            let! ch = Cloud.StartChild(Cloud.CurrentWorker, target = currentWorker)
            let! result = ch
            return result = currentWorker
        } |> run |> Choice.shouldEqual true


    [<Test>]
    let ``4. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (runtime.Value.Workers.Length)

    [<Test>]
    let ``4. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> Choice.shouldMatch (fun _ -> true)

    [<Test>]
    let ``4. Runtime : Get process id`` () =
        run (Cloud.GetProcessId()) |> Choice.shouldMatch (fun _ -> true)

    [<Test>]
    let ``4. Runtime : Get task id`` () =
        run (Cloud.GetTaskId()) |> Choice.shouldMatch (fun _ -> true)

    [<Test>]
    [<Repeat(repeats)>]
    let ``5. Fault Tolerance : map/reduce`` () =
        let t = runtime.Value.RunAsTask(wordCount 20 mapReduceRec)
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        t.Result |> should equal 100

    [<Test>]
    [<Repeat(repeats)>]
    let ``5. Fault Tolerance : Custom fault policy 1`` () =
        let t = runtime.Value.RunAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>

    [<Test>]
    [<Repeat(repeats)>]
    let ``5. Fault Tolerance : Custom fault policy 2`` () =
        let t = runtime.Value.RunAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>

    [<Test>]
    let ``6. Channels : simple send/receive`` () =
        cloud {
            let! send,recv = CloudChannel.New<int> ()
            let! _,value = CloudChannel.Send 42 send <||> CloudChannel.Receive recv
            return value
        } |> run |> Choice.shouldEqual 42

    [<Test>]
    let ``6. Channels : multiple send/receive`` () =
        cloud {
            let! sp,rp = CloudChannel.New<int option> ()
            let rec sender n = cloud {
                if n = 0 then
                    do! CloudChannel.Send None sp
                else
                    do! CloudChannel.Send (Some n) sp
                    return! sender (n-1)
            }

            let rec receiver c = cloud {
                let! v = CloudChannel.Receive rp
                match v with
                | None -> return c
                | Some i ->
                    printfn "RECEIVED : %d" i
                    return! receiver (c + i)
            }

            let! _, result = sender 100 <||> receiver 0
            return result
        } |> run |> Choice.shouldEqual 5050

    [<Test>]
    let ``6. Channels : multiple senders`` () =
        cloud {
            let! sp, rp = CloudChannel.New<int> ()
            let sender n = cloud {
                for i in 1 .. n do
                    do! CloudChannel.Send i sp
            }

            let rec receiver c n = cloud {
                if n = 0 then return c
                else
                    let! i = CloudChannel.Receive rp
                    return! receiver (c + i) (n - 1)
            }

            let senders = Seq.init 10 (fun _ -> sender 10) |> Cloud.Parallel |> Cloud.Ignore
            let! _,result = senders <||> receiver 0 100
            return result
        } |> run |> Choice.shouldEqual 550