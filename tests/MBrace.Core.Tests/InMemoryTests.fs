namespace Nessos.MBrace.Tests
    
    open System
    open System.Threading

    open NUnit.Framework
    open FsUnit

    open Nessos.MBrace
    open Nessos.MBrace.InMemory

    [<TestFixture>]
    module InMemoryTests =
        
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
                let f _ = cloud { return invalidOp "failure" }
                try
                    let! _ = Array.init 100 f |> Cloud.Parallel
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