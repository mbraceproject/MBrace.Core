namespace Nessos.MBrace.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.SampleRuntime
open Nessos.MBrace.SampleRuntime.Actors

[<TestFixture>]
module ``Distribution Tests`` =

    type Canceller = Actors.EventActor<unit>

    let mutable runtime : MBraceRuntime option = None

    [<TestFixtureSetUp>]
    let init () =
        MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"
        runtime <- Some <| MBraceRuntime.InitLocal(4)

    [<TestFixtureTearDown>]
    let fini () =
        runtime |> Option.iter (fun r -> r.Kill())
        runtime <- None

    let run (workflow : Cloud<'T>) = Option.get(runtime).RunAsync workflow |> Async.Catch |> Async.RunSynchronously
    let runCts (workflow : Canceller -> Cloud<'T>) =
        let cts = new CancellationTokenSource()
        let observable, canceller = Canceller.Init()
        let _ = observable.Subscribe(fun () -> cts.Cancel())
        Option.get(runtime).RunAsync(workflow canceller, cancellationToken = cts.Token) |> Async.Catch |> Async.RunSynchronously

    [<Test>]
    let ``Parallel : empty input`` () =
        run (Cloud.Parallel [||]) |> Choice.shouldEqual [||]

    [<Test>]
    let ``Parallel : simple inputs`` () =
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual 210

    [<Test>]
    let ``Parallel : simple nested`` () =
        cloud {
            let f i j = cloud { return i + j + 2 }
            let cluster i = Array.init 10 (f i) |> Cloud.Parallel
            let! results = Array.init 10 cluster |> Cloud.Parallel
            return Array.concat results |> Array.sum
        } |> run |> Choice.shouldEqual 1100

    [<Test>]
    let ``Parallel : simple exception`` () =
        cloud {
            let f i = cloud { return if i = 15 then invalidOp "failure" else i + 1 }
            let! results = Array.init 20 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    [<Repeat(4)>]
    let ``Parallel : exception contention`` () =
        let latch = Latch.Init(0)
        cloud {
            try
                let! _ = Array.init 20 (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                latch.Increment() |> ignore
                return ()
        } |> run |> Choice.shouldEqual ()

        // test that exception continuation was fired precisely once
        latch.Value |> should equal 1


    [<Test>]
    [<Repeat(4)>]
    let ``Parallel : exception cancellation`` () =
        cloud {
            let latch = Latch.Init 0
            let worker i = cloud { 
                if i = 0 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = latch.Increment()
                    return ()
            }

            try
                let! _ = Array.init 20 worker |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                return latch.Value
        } |> run |> Choice.shouldMatch(fun i -> i < 5)

    [<Test>]
    [<Repeat(4)>]
    let ``Parallel : nested exception cancellation`` () =
        cloud {
            let latch = Latch.Init 0
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = latch.Increment()
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
    [<Repeat(4)>]
    let ``Parallel : cancellation`` () =
        let latch = Latch.Init 0
        runCts(fun cts -> cloud {
            let parallelTasks = cloud {
                let f i = cloud { 
                    do! Cloud.Sleep 1000 
                    return latch.Increment() }

                do! Array.init 10 f |> Cloud.Parallel |> Cloud.Ignore
            }

            let! _ =  parallelTasks <||> cloud { cts.Trigger() }

            return ()
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        latch.Value |> should equal 0


    [<Test>]
    [<Repeat(4)>]
    let ``Parallel : to local`` () =
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
    [<Repeat(4)>]
    let ``Parallel : to sequential`` () =
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


    [<Test>]
    let ``Choice : empty input`` () =
        Cloud.Choice [] |> run |> Choice.shouldEqual None

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : all inputs 'None'`` () =
        cloud {
            let count = Latch.Init 0
            let worker _ = cloud {
                let _ = count.Increment()
                return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return (count.Value, result)
        } |> run |> Choice.shouldEqual (20, None)


    [<Test>]
    [<Repeat(4)>]
    let ``Choice : one input 'Some'`` () =
        cloud {
            let count = Latch.Init 0
            let worker i = cloud {
                if i = 0 then return Some i
                else
                    do! Cloud.Sleep 1000
                    // check proper cancellation while we're at it.
                    let _ = count.Increment()
                    return None
            }

            let! result = Array.init 20 worker |> Cloud.Choice
            return result, count.Value
        } |> run |> Choice.shouldEqual (Some 0, 0)

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : all inputs 'Some'`` () =
        let successcounter = Latch.Init 0
        cloud {
            let worker _ = cloud { return Some 42 }
            let! result = Array.init 20 worker |> Cloud.Choice
            let _ = successcounter.Increment()
            return result
        } |> run |> Choice.shouldEqual (Some 42)

        // ensure only one success continuation call
        successcounter.Value |> should equal 1

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : simple nested`` () =
        let counter = Latch.Init 0
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    return Some(i,j)
                else
                    do! Cloud.Sleep 100
                    let _ = counter.Increment()
                    return None
            }

            let cluster i = Array.init 4 (worker i) |> Cloud.Choice
            let! result = Array.init 5 cluster |> Cloud.Choice
            return result
        } |> run |> Choice.shouldEqual (Some (0,0))

        counter.Value |> should be (lessThan 30)

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : nested exception cancellation`` () =
        let counter = Latch.Init 0
        cloud {
            let worker i j = cloud {
                if i = 0 && j = 0 then
                    return invalidOp "failure"
                else
                    do! Cloud.Sleep 10000
                    let _ = counter.Increment()
                    return Some 42
            }

            let cluster i = Array.init 5 (worker i) |> Cloud.Choice
            return! Array.init 4 cluster |> Cloud.Choice
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        counter.Value |> should be (lessThan 30)

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : simple cancellation`` () =
        let taskCount = Latch.Init 0
        runCts(fun cts ->
            cloud {
                let worker i = cloud {
                    if i = 0 then
                        cts.Trigger()
                        do! Cloud.Sleep 1000
                        return Some 42
                    else
                        do! Cloud.Sleep 1000
                        let _ = taskCount.Increment()
                        return Some 42
                }

                return! Array.init 10 worker |> Cloud.Choice
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        taskCount.Value |> should equal 0

    [<Test>]
    [<Repeat(4)>]
    let ``Choice : to local`` () =
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
    [<Repeat(4)>]
    let ``Choice : to sequential`` () =
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
    [<Repeat(4)>]
    let ``StartChild: task with success`` () =
        cloud {
            let count = Latch.Init 0
            let task = cloud {
                do! Cloud.Sleep 100
                return count.Increment()
            }

            let! ch = Cloud.StartChild(task)
            count.Value |> should equal 0
            return! ch
        } |> run |> Choice.shouldEqual 1

    [<Test>]
    [<Repeat(4)>]
    let ``StartChild: task with exception`` () =
        let count = Latch.Init 0
        cloud {
            let task = cloud {
                do! Cloud.Sleep 100
                let _ = count.Increment()
                return invalidOp "failure"
            }

            let! ch = Cloud.StartChild(task)
            count.Value |> should equal 0
            do! Cloud.Sleep 100
            // ensure no exception is raised in parent workflow
            // before the child workflow is properly evaluated
            let _ = count.Increment()
            return! ch
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        count.Value |> should equal 2

    [<Test>]
    [<Repeat(4)>]
    let ``StartChild: task with cancellation`` () =
        let count = Latch.Init 0
        runCts(fun cts ->
            cloud {
                let task = cloud {
                    do! Cloud.Sleep 200
                    let _ = count.Increment()
                    cts.Trigger ()
                    do! Cloud.Sleep 200
                    do! Cloud.Sleep 200
                    return count.Increment()
                }

                let! ch = Cloud.StartChild(task)
                count.Value |> should equal 0
                return! ch
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        // ensure final increment was cancelled.
        count.Value |> should equal 1