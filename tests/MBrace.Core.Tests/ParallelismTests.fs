namespace MBrace.Tests
    
open System
open System.Threading

open NUnit.Framework

open MBrace
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Runtime.InMemory

/// Distributed Cancellation token source abstraction
type ICancellationTokenSource =
    abstract Cancel : unit -> unit

/// Logging tester abstraction
type ILogTester =
    abstract Clear : unit -> unit
    abstract GetLogs : unit -> string []

/// Suite for testing MBrace parallelism & distribution
[<TestFixture>]
[<AbstractClass>]
type ``Parallelism Tests`` (nParallel : int) as self =

    let nNested = nParallel |> float |> ceil |> int

    let repeat f = repeat self.Repeats f

    let run (workflow : Cloud<'T>) = self.Run workflow
    let runCts (workflow : ICancellationTokenSource -> Cloud<'T>) = self.Run workflow
    let runLocal (workflow : Cloud<'T>) = self.RunLocal workflow
    
    /// Run workflow in the runtime under test
    abstract Run : workflow:Cloud<'T> -> Choice<'T, exn>
    /// Run workflow in the runtime under test, with cancellation token source passed to the worker
    abstract Run : workflow:(ICancellationTokenSource -> Cloud<'T>) -> Choice<'T, exn>
    /// Evaluate workflow in the local test process
    abstract RunLocal : workflow:Cloud<'T> -> 'T
    /// Maximum number of tests to be run by FsCheck
    abstract FsCheckMaxTests : int
    /// Maximum number of repeats to run nondeterministic tests
    abstract Repeats : int
    /// Enables targeted worker tests
    abstract IsTargetWorkerSupported : bool
    /// Log tester
    abstract Logs : ILogTester

    [<Test>]
    member __.``1. Parallel : empty input`` () =
        Array.empty<Cloud<int>> |> Cloud.Parallel |> run |> Choice.shouldEqual [||]

    [<Test>]
    member __.``1. Parallel : simple inputs`` () =
        let nParallel = nParallel
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Seq.init nParallel f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual (Seq.init nParallel (fun i -> i + 1) |> Seq.sum)

    [<Test>]
    member __.``1. Parallel : random inputs`` () =
        let checker (ints:int[]) =
            if ints = null then () else
            let maxSize = 5 * nParallel
            let ints = if ints.Length <= maxSize then ints else ints.[..maxSize]
            cloud {
                let f i = cloud { return ints.[i] }
                return! Seq.init ints.Length f |> Cloud.Parallel
            } |> run |> Choice.shouldEqual ints

        Check.QuickThrowOnFail(checker, maxRuns = self.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : use binding`` () =
        let nParallel = nParallel
        let c = CloudAtom.New 0 |> runLocal
        cloud {
            use foo = { new ICloudDisposable with member __.Dispose () = CloudAtom.Incr c }
            let! _ = Seq.init nParallel (fun _ -> CloudAtom.Incr c) |> Cloud.Parallel
            return! c.Value
        } |> run |> Choice.shouldEqual nParallel

        c.Value |> runLocal |> shouldEqual (nParallel + 1)

    [<Test>]
    member  __.``1. Parallel : exception handler`` () =
        cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            with :? InvalidOperationException as e ->
                let! x,y = cloud { return 1 } <||> cloud { return 2 }
                return x + y
        } |> run |> Choice.shouldEqual 3

    [<Test>]
    member  __.``1. Parallel : finally`` () =
        let trigger = runLocal <| CloudAtom.New 0
        Cloud.TryFinally( cloud {
            let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
            return () }, CloudAtom.Incr trigger)
        |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        trigger.Value |> runLocal |> shouldEqual 1

    [<Test>]
    member __.``1. Parallel : simple nested`` () =
        let nNested = nNested
        cloud {
            let f i j = cloud { return i + j + 1 }
            let cluster i = Array.init nNested (f i) |> Cloud.Parallel
            let! results = Array.init nNested cluster |> Cloud.Parallel
            return Array.concat results |> Array.sum
        } |> run |> Choice.shouldEqual (Seq.init nNested (fun i -> Seq.init nNested (fun j -> i + j + 1)) |> Seq.concat |> Seq.sum)
            
    [<Test>]
    member __.``1. Parallel : simple exception`` () =
        let nParallel = nParallel
        cloud {
            let f i = cloud { return if i = nParallel / 2 then invalidOp "failure" else i + 1 }
            let! results = Array.init nParallel f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    member __.``1. Parallel : exception contention`` () =
        repeat(fun () ->
            let nParallel = nParallel
            // test that exception continuation was fired precisely once
            cloud {
                let! atom = CloudAtom.New 0
                try                    
                    let! _ = Array.init nParallel (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    do! CloudAtom.Incr atom
                    return ()

                do! Cloud.Sleep 500
                return! CloudAtom.Read atom
            } |> run |> Choice.shouldEqual 1)

    [<Test>]
    member __.``1. Parallel : exception cancellation`` () =
        repeat(fun () ->
            cloud {
                let! counter = CloudAtom.New 0
                let worker i = cloud { 
                    if i = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep 2000
                        do! CloudAtom.Incr counter
                }

                try
                    let! _ = Array.init 20 worker |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    return! counter.Value
            } |> run |> Choice.shouldEqual 0)

    [<Test>]
    member __.``1. Parallel : nested exception cancellation`` () =
        repeat(fun () ->
            cloud {
                let! counter = CloudAtom.New 0
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep 3000
                        do! CloudAtom.Incr counter
                }

                let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
                try
                    do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    return! counter.Value

            } |> run |> Choice.shouldEqual 0)
            

    [<Test>]
    member __.``1. Parallel : simple cancellation`` () =
        repeat(fun () ->
            let counter = CloudAtom.New 0 |> runLocal
            runCts(fun cts -> cloud {
                let f i = cloud {
                    if i = 0 then cts.Cancel() 
                    do! Cloud.Sleep 3000 
                    do! CloudAtom.Incr counter
                }

                let! _ = Array.init 10 f |> Cloud.Parallel

                return ()
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

            counter.Value |> runLocal |> shouldEqual 0)

    [<Test>]
    member __.``1. Parallel : to local`` () =
        repeat(fun () ->
            // check local semantics are forced by using ref cells.
            cloud {
                let counter = ref 0
                let seqWorker _ = cloud {
                    do! Cloud.Sleep 10
                    Interlocked.Increment counter |> ignore
                }

                let! results = Array.init 100 seqWorker |> Cloud.Parallel |> Cloud.ToLocal
                return counter.Value
            } |> run |> Choice.shouldEqual 100)

    [<Test>]
    member __.``1. Parallel : to sequential`` () =
        repeat(fun () ->
            // check sequential semantics are forced by deliberately
            // making use of code that is not thread-safe.
            cloud {
                let counter = ref 0
                let seqWorker _ = cloud {
                    let init = !counter + 1
                    counter := init
                    return !counter = init
                }

                let! results = Array.init 100 seqWorker |> Cloud.Parallel |> Cloud.ToSequential
                return Array.forall id results
            } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``1. Parallel : MapReduce recursive`` () =
        // naive, binary recursive mapreduce implementation
        repeat(fun () -> WordCount.run 20 WordCount.mapReduceRec |> run |> Choice.shouldEqual 100)

    [<Test>]
    member __.``1. Parallel : MapReduce balanced`` () =
        // balanced, core implemented MapReduce algorithm
        repeat(fun () -> WordCount.run 1000 Distributed.mapReduce |> run |> Choice.shouldEqual 5000)

    [<Test>]
    member __.``1. Parallel : Distributed.map`` () =
        let checker (ints : int list) =
            let expected = ints |> List.map (fun i -> i + 1) |> List.toArray
            ints
            |> Distributed.map (fun i -> cloud { return i + 1})
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Distributed.filter`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 5 = 0 || i % 7 = 0) |> List.toArray
            ints
            |> Distributed.filter (fun i -> cloud { return i % 5 = 0 || i % 7 = 0 })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Distributed.choose`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 5 = 0 || i % 7 = 0 then Some i else None) |> List.toArray
            ints
            |> Distributed.choose (fun i -> cloud { return if i % 5 = 0 || i % 7 = 0 then Some i else None })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Distributed.fold`` () =
        let checker (ints : int list) =
            let expected = ints |> List.fold (fun s i -> s + i) 0
            ints
            |> Distributed.fold (Cloud.lift2 (fun s i -> s + i)) (Cloud.lift2 (fun s i -> s + i)) 0
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! results = Cloud.Parallel Cloud.CurrentWorker
                    return set results = set workers
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``1. Parallel : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Parallel [(Cloud.CurrentWorker, thisWorker)]
                    return results.[0] = thisWorker
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``1. Parallel : to all workers in local semantics`` () =
        if __.IsTargetWorkerSupported then
            Cloud.CurrentWorker
            |> Cloud.Parallel
            |> Cloud.ToLocal
            |> run
            |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    member __.``2. Choice : empty input`` () =
        Cloud.Choice List.empty<Cloud<int option>> |> run |> Choice.shouldEqual None

    [<Test>]
    member __.``2. Choice : random inputs`` () =
        let checker (size : bool list) =
            let expected = size |> Seq.mapi (fun i b -> (i,b)) |> Seq.filter snd |> Seq.map fst |> set
            let worker i b = cloud { return if b then Some i else None }
            size 
            |> Seq.mapi worker 
            |> Cloud.Choice
            |> run 
            |> Choice.shouldBe (function Some r -> expected.Contains r | None -> Set.isEmpty expected)

        Check.QuickThrowOnFail(checker, maxRuns = self.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : all inputs 'None'`` () =
        repeat(fun () ->
            let nParallel = nParallel
            let count = CloudAtom.New 0 |> runLocal
            cloud {
                let worker _ = cloud {
                    do! CloudAtom.Incr count
                    return None
                }

                return! Array.init nParallel worker |> Cloud.Choice
            } |> run |> Choice.shouldEqual None

            count.Value |> runLocal |> shouldEqual nParallel)

    [<Test>]
    member __.``2. Choice : one input 'Some'`` () =
        repeat(fun () ->
            let nParallel = nParallel
            let count = CloudAtom.New 0 |> runLocal
            cloud {
                let worker i = cloud {
                    if i = 0 then return Some i
                    else
                        do! Cloud.Sleep 3000
                        // check proper cancellation while we're at it.
                        do! CloudAtom.Incr count
                        return None
                }

                return! Array.init nParallel worker |> Cloud.Choice
            } |> run |> Choice.shouldEqual (Some 0)
            count.Value |> runLocal |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : all inputs 'Some'`` () =
        repeat(fun () ->
            let successcounter = CloudAtom.New 0 |> runLocal
            cloud {
                let worker _ = cloud { return Some 42 }
                let! result = Array.init 100 worker |> Cloud.Choice
                do! CloudAtom.Incr successcounter
                return result
            } |> run |> Choice.shouldEqual (Some 42)

            // ensure only one success continuation call
            successcounter.Value |> runLocal |> shouldEqual 1)

    [<Test>]
    member __.``2. Choice : simple nested`` () =
        repeat(fun () ->
            let nParallel = nParallel
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runLocal
            cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return Some(i,j)
                    else
                        do! Cloud.Sleep 3000
                        do! CloudAtom.Incr counter
                        return None
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } |> run |> Choice.shouldEqual (Some(0,0))

            counter.Value |> runLocal |> shouldBe (fun i ->  i < nParallel / 2))

    [<Test>]
    member __.``2. Choice : nested exception cancellation`` () =
        repeat(fun () ->
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runLocal
            cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return invalidOp "failure"
                    else
                        do! Cloud.Sleep 3000
                        do! CloudAtom.Incr counter
                        return Some 42
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

            counter.Value |> runLocal |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : simple cancellation`` () =
        repeat(fun () ->
            let nParallel = nParallel
            let counter = CloudAtom.New 0 |> runLocal
            runCts(fun cts ->
                cloud {
                    let worker i = cloud {
                        if i = 0 then cts.Cancel()
                        do! Cloud.Sleep 3000
                        do! CloudAtom.Incr counter
                        return Some 42
                    }

                    return! Array.init nParallel worker |> Cloud.Choice
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

            counter.Value |> runLocal |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : to local`` () =
        repeat(fun () ->
            let nParallel = nParallel
            // check local semantics are forced by using ref cells.
            cloud {
                let counter = ref 0
                let seqWorker i = cloud {
                    if i = nParallel / 2 then
                        do! Cloud.Sleep 100
                        return Some i
                    else
                        let _ = Interlocked.Increment counter
                        return None
                }

                let! result = Array.init nParallel seqWorker |> Cloud.Choice |> Cloud.ToLocal
                counter.Value |> shouldEqual (nParallel - 1)
                return result
            } |> run |> Choice.shouldEqual (Some (nParallel / 2)))

    [<Test>]
    member __.``2. Choice : to Sequential`` () =
        repeat(fun () ->
            // check sequential semantics are forced by deliberately
            // making use of code that is not thread-safe.
            cloud {
                let counter = ref 0
                let seqWorker i = cloud {
                    let init = !counter + 1
                    counter := init
                    do! Cloud.Sleep 10
                    !counter |> shouldEqual init
                    if i = 16 then
                        return Some ()
                    else
                        return None
                }

                let! result = Array.init 20 seqWorker |> Cloud.Choice |> Cloud.ToSequential
                return result, !counter
            } |> run |> Choice.shouldEqual (Some(), 17))

    [<Test>]
    member __.``2. Choice : Distributed.tryFind`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 7 = 0 && i % 5 = 0) |> set
            ints
            |> Distributed.tryFind (fun i -> cloud { return i % 7 = 0 && i % 5 = 0 })
            |> run
            |> Choice.shouldBe(function None -> Set.isEmpty expected | Some r -> expected.Contains r)

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : Distributed.tryPick`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 7 = 0 && i % 5 = 0 then Some i else None) |> set
            ints
            |> Distributed.tryPick (fun i -> cloud { return if i % 7 = 0 && i % 5 = 0 then Some i else None })
            |> run
            |> Choice.shouldBe (function None -> Set.isEmpty expected | Some r -> expected.Contains r)

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! counter = CloudAtom.New 0
                    let! _ = Cloud.Choice (cloud { let! _ = CloudAtom.Incr counter in return Option<int>.None })
                    let! value = counter.Value
                    return value = workers.Length
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``2. Choice : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Choice [(cloud { let! w = Cloud.CurrentWorker in return Some w }, thisWorker)]
                    return results.Value = thisWorker
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``2. Choice : to all workers in local semantics`` () =
        if __.IsTargetWorkerSupported then
            cloud { return Some 42 }
            |> Cloud.Choice
            |> Cloud.ToLocal
            |> run
            |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    member __.``3. StartChild: task with success`` () =
        repeat(fun () ->
            cloud {
                use! count = CloudAtom.New 0
                let task = cloud {
                    do! Cloud.Sleep 1000
                    do! CloudAtom.Incr count
                    return! count.Value
                }

                let! ch = Cloud.StartChild(task)
                let! value = count.Value
                value |> shouldEqual 0
                return! ch
            } |> run |> Choice.shouldEqual 1)

    [<Test>]
    member __.``3. StartChild: task with exception`` () =
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runLocal
            cloud {
                let task = cloud {
                    do! Cloud.Sleep 1000
                    do! CloudAtom.Incr count
                    return invalidOp "failure"
                }

                let! ch = Cloud.StartChild(task)
                let! value = count.Value
                value |> shouldEqual 0
                do! Cloud.Sleep 100
                // ensure no exception is raised in parent workflow
                // before the child workflow is properly evaluated
                do! CloudAtom.Incr count
                return! ch
            } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

            count.Value |> runLocal |> shouldEqual 2)

    [<Test>]
    member __.``3. StartChild: task with cancellation`` () =
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runLocal
            runCts(fun cts ->
                cloud {
                    let task = cloud {
                        do! CloudAtom.Incr count
                        do! Cloud.Sleep 3000
                        do! CloudAtom.Incr count
                    }

                    let! ch = Cloud.StartChild(task)
                    do! Cloud.Sleep 1000
                    let! value = count.Value
                    value |> shouldEqual 1
                    cts.Cancel ()
                    return! ch
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

            // ensure final increment was cancelled.
            count.Value |> runLocal |> shouldEqual 1)

    [<Test>]
    member __.``3. StartChild: to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! currentWorker = Cloud.CurrentWorker
                    let! ch = Cloud.StartChild(Cloud.CurrentWorker, target = currentWorker)
                    let! result = ch
                    return result = currentWorker
                } |> run |> Choice.shouldEqual true)


    [<Test>]
    member __.``4. Logging`` () =
        __.Logs.Clear()
        cloud {
            let logSeq _ = cloud {
                for i in [1 .. 100] do
                    do! Cloud.Logf "message %d" i
            }

            do! Seq.init 20 logSeq |> Cloud.Parallel |> Cloud.Ignore
            do! Cloud.Sleep 1000
        } |> __.Run |> ignore
        
        __.Logs.GetLogs().Length |> shouldEqual 2000