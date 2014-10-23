namespace Nessos.MBrace.Tests
    
open System
open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.InMemory

[<TestFixture>]
module ``In-Memory Parallelism Tests`` =
        
    let run (workflow : Cloud<'T>) = Cloud.RunProtected(workflow, resources = InMemoryRuntime.Resource)
    let runCts (workflow : CancellationTokenSource -> Cloud<'T>) = Cloud.RunProtected(workflow, resources = InMemoryRuntime.Resource)


    [<Test>]
    let ``Parallel : empty input`` () =
        run (Cloud.Parallel [||]) |> Choice.shouldEqual [||]

    [<Test>]
    let ``Parallel : simple inputs`` () =
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Array.init 100 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual 5050

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
            let f i = cloud { return if i = 55 then invalidOp "failure" else i + 1 }
            let! results = Array.init 100 f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    [<Repeat(10)>]
    let ``Parallel : exception contention`` () =
        let counter = ref 0
        cloud {
            try
                let! _ = Array.init 100 (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                Interlocked.Increment counter |> ignore
                return ()
        } |> run |> Choice.shouldEqual ()

        // test that exception continuation was fired precisely once
        !counter |> should equal 1

    [<Test>]
    [<Repeat(10)>]
    let ``Parallel : exception cancellation`` () =
        cloud {
            let counter = ref 0
            let worker i = cloud { 
                if i = 13 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = Interlocked.Increment counter
                    return ()
            }

            try
                let! _ = Array.init 20 worker |> Cloud.Parallel
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                return !counter
        } |> run |> Choice.shouldEqual 0

    [<Test>]
    [<Repeat(10)>]
    let ``Parallel : nested exception cancellation`` () =
        cloud {
            let counter = ref 0
            let worker i j = cloud {
                if i = 6 && j = 5 then
                    do! Cloud.Sleep 100
                    invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = Interlocked.Increment counter
                    return ()
            }

            let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
            try
                do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
                return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
            with :? InvalidOperationException ->
                return !counter
        } |> run |> Choice.shouldEqual 0
            

    [<Test>]
    [<Repeat(10)>]
    let ``Parallel : cancellation`` () =
        let counter = ref 0
        runCts(fun cts -> cloud {
            let parallelTasks = cloud {
                let f i = cloud { 
                    do! Cloud.Sleep 1000 
                    return Interlocked.Increment counter }

                do! Array.init 10 f |> Cloud.Parallel |> Cloud.Ignore
            }

            let! _ = 
                parallelTasks <||> 
                    cloud { 
                        do! Cloud.Sleep 100 
                        cts.Cancel () 
                    }

            return ()
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !counter |> should equal 0

    [<Test>]
    [<Repeat(10)>]
    let ``Parallel : to sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker _ = cloud {
                let init = !counter + 1
                counter := init
                do! Cloud.Sleep 10
                return !counter = init
            }

            let! results = Array.init 20 seqWorker |> Cloud.Parallel |> Cloud.ToSequential
            return Array.forall id results
        } |> run |> Choice.shouldEqual true


    [<Test>]
    let ``Choice : empty input`` () =
        Cloud.Choice [] |> run |> Choice.shouldEqual None

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : all inputs 'None'`` () =
        cloud {
            let count = ref 0
            let worker _ = cloud {
                let _ = Interlocked.Increment count
                return None
            }

            let! result = Array.init 100 worker |> Cloud.Choice
            return (!count, result)
        } |> run |> Choice.shouldEqual (100, None)

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : one input 'Some'`` () =
        cloud {
            let count = ref 0
            let worker i = cloud {
                if i = 15 then return Some i
                else
                    do! Cloud.Sleep 100
                    // check proper cancellation while we're at it.
                    let _ = Interlocked.Increment count
                    return None
            }

            let! result = Array.init 100 worker |> Cloud.Choice
            return result, !count
        } |> run |> Choice.shouldEqual (Some 15, 0)

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : all inputs 'Some'`` () =
        let successcounter = ref 0
        cloud {
            let worker _ = cloud { return Some 42 }
            let! result = Array.init 100 worker |> Cloud.Choice
            let _ = Interlocked.Increment successcounter
            return result
        } |> run |> Choice.shouldEqual (Some 42)

        // ensure only one success continuation call
        !successcounter |> should equal 1

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : simple nested`` () =
        cloud {
            let counter = ref 0
            let worker i j = cloud {
                if i = 5 && j = 3 then
                    do! Cloud.Sleep 100
                    return Some(i,j)
                else
                    do! Cloud.Sleep 1000
                    let _ = Interlocked.Increment counter
                    return None
            }

            let cluster i = Array.init 10 (worker i) |> Cloud.Choice
            let! result = Array.init 10 cluster |> Cloud.Choice
            return !counter = 0, result
        } |> run |> Choice.shouldEqual (true, Some (5,3))

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : nested exception cancellation`` () =
        let counter = ref 0
        cloud {
            let worker i j = cloud {
                if i = 5 && j = 3 then
                    do! Cloud.Sleep 100
                    return invalidOp "failure"
                else
                    do! Cloud.Sleep 1000
                    let _ = Interlocked.Increment counter
                    return Some 42
            }

            let cluster i = Array.init 10 (worker i) |> Cloud.Choice
            return! Array.init 10 cluster |> Cloud.Choice
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        !counter |> should equal 0

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : simple cancellation`` () =
        let taskCount = ref 0
        runCts(fun cts ->
            cloud {
                let worker i = cloud {
                    if i = 55 then
                        do! Cloud.Sleep 10
                        cts.Cancel()
                        return Some 42
                    else
                        do! Cloud.Sleep 100
                        let _ = Interlocked.Increment taskCount
                        return Some 42
                }

                return! Array.init 100 worker |> Cloud.Choice
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !taskCount |> should equal 0

    [<Test>]
    [<Repeat(10)>]
    let ``Choice : to Sequential`` () =
        // check sequential semantics are forced by deliberately
        // making use of code that is not thread-safe.
        cloud {
            let counter = ref 0
            let seqWorker i = cloud {
                let init = !counter + 1
                counter := init
                do! Cloud.Sleep 10
                !counter |> should equal init
                if i = 16 then
                    return Some ()
                else
                    return None
            }

            let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToSequential
            return result, !counter
        } |> run |> Choice.shouldEqual (Some(), 17)

    [<Test>]
    [<Repeat(10)>]
    let ``StartChild: task with success`` () =
        cloud {
            let count = ref 0
            let task = cloud {
                do! Cloud.Sleep 100
                return Interlocked.Increment count
            }

            let! ch = Cloud.StartChild(task)
            !count |> should equal 0
            return! ch
        } |> run |> Choice.shouldEqual 1

    [<Test>]
    [<Repeat(10)>]
    let ``StartChild: task with exception`` () =
        let count = ref 0
        cloud {
            let task = cloud {
                do! Cloud.Sleep 100
                let _ = Interlocked.Increment count
                return invalidOp "failure"
            }

            let! ch = Cloud.StartChild(task)
            !count |> should equal 0
            do! Cloud.Sleep 100
            // ensure no exception is raised in parent workflow
            // before the child workflow is properly evaluated
            let _ = Interlocked.Increment count
            return! ch
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        !count |> should equal 2

    [<Test>]
    [<Repeat(10)>]
    let ``StartChild: task with cancellation`` () =
        let count = ref 0
        runCts(fun cts ->
        cloud {
            let task = cloud {
                do! Cloud.Sleep 100
                let _ = Interlocked.Increment count
                cts.Cancel()
                return! cloud { return Interlocked.Increment count }
            }

            let! ch = Cloud.StartChild(task)
            !count |> should equal 0
            return! ch
        }) |> Choice.shouldFailwith<_, OperationCanceledException>

        // ensure final increment was cancelled.
        !count |> should equal 1

    [<Test>]
    [<Repeat(10)>]
    let ``StartChild: task with timeout`` () =
        let counter = ref 0
        cloud {
            let task = cloud {
                incr counter
                do! Cloud.Sleep 1000
                incr counter
            }

            let! ch = Cloud.StartChild(task, timeoutMilliseconds = 100)
            do! Cloud.Sleep 20
            !counter |> should equal 1
            return! ch
        } |> run |> Choice.shouldFailwith<_, TimeoutException>

        !counter |> should equal 1