namespace MBrace.Core.Tests
    
open System
open System.Threading
open System.Runtime.Serialization

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime
open MBrace.Store
open MBrace.Workflows

/// Logging tester abstraction
type ILogTester =
    abstract Clear : unit -> unit
    abstract GetLogs : unit -> string []

/// Suite for testing MBrace parallelism & distribution
/// <param name="parallelismFactor">Maximum permitted parallel jobs permitted in tests.</param>
/// <param name="delayFactor">
///     Delay factor in milliseconds used by unit tests. 
///     Use a value that ensures propagation of updates across the cluster.
/// </param>
[<TestFixture>]
[<AbstractClass>]
type ``Parallelism Tests`` (parallelismFactor : int, delayFactor : int) as self =

    let nNested = parallelismFactor |> float |> ceil |> int

    let repeat f = repeat self.Repeats f

    let run (workflow : Cloud<'T>) = self.Run workflow
    let runCts (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = self.Run workflow
    let runLocally (workflow : Cloud<'T>) = self.RunLocally workflow
    
    /// Run workflow in the runtime under test
    abstract Run : workflow:Cloud<'T> -> Choice<'T, exn>
    /// Run workflow in the runtime under test, with cancellation token source passed to the worker
    abstract Run : workflow:(ICloudCancellationTokenSource -> #Cloud<'T>) -> Choice<'T, exn>
    /// Evaluate workflow in the local test process
    abstract RunLocally : workflow:Cloud<'T> -> 'T
    /// Maximum number of tests to be run by FsCheck
    abstract FsCheckMaxTests : int
    /// Maximum number of repeats to run nondeterministic tests
    abstract Repeats : int
    /// Enables targeted worker tests
    abstract IsTargetWorkerSupported : bool
    /// Declares that this runtime uses serialization/distribution
    abstract UsesSerialization : bool
    /// Log tester
    abstract Logs : ILogTester

    //
    //  1. Parallelism tests
    //

    [<Test>]
    member __.``1. Parallel : empty input`` () =
        Array.empty<Cloud<int>> |> Cloud.Parallel |> run |> Choice.shouldEqual [||]

    [<Test>]
    member __.``1. Parallel : simple inputs`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Seq.init parallelismFactor f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldEqual (Seq.init parallelismFactor (fun i -> i + 1) |> Seq.sum)

    [<Test>]
    member __.``1. Parallel : random inputs`` () =
        let checker (ints:int[]) =
            if ints = null then () else
            let maxSize = 5 * parallelismFactor
            let ints = if ints.Length <= maxSize then ints else ints.[..maxSize]
            cloud {
                let f i = cloud { return ints.[i] }
                return! Seq.init ints.Length f |> Cloud.Parallel
            } |> run |> Choice.shouldEqual ints

        Check.QuickThrowOnFail(checker, maxRuns = self.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : use binding`` () =
        let parallelismFactor = parallelismFactor
        let c = CloudAtom.New 0 |> runLocally
        cloud {
            use foo = { new ICloudDisposable with member __.Dispose () = CloudAtom.Incr c |> Local.Ignore }
            let! _ = Seq.init parallelismFactor (fun _ -> CloudAtom.Incr c) |> Cloud.Parallel
            return! c.Value
        } |> run |> Choice.shouldEqual parallelismFactor

        c.Value |> runLocally |> shouldEqual (parallelismFactor + 1)

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
        let trigger = runLocally <| CloudAtom.New 0
        Cloud.TryFinally( cloud {
            let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
            return () }, CloudAtom.Incr trigger |> Local.Ignore)
        |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        trigger.Value |> runLocally |> shouldEqual 1

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
        let parallelismFactor = parallelismFactor
        cloud {
            let f i = cloud { return if i = parallelismFactor / 2 then invalidOp "failure" else i + 1 }
            let! results = Array.init parallelismFactor f |> Cloud.Parallel
            return Array.sum results
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    member __.``1. Parallel : exception contention`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // test that exception continuation was fired precisely once
            cloud {
                let! atom = CloudAtom.New 0
                try                    
                    let! _ = Array.init parallelismFactor (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    let! _ = CloudAtom.Incr atom
                    return ()

                do! Cloud.Sleep 500
                return! CloudAtom.Read atom
            } |> run |> Choice.shouldEqual 1)

    [<Test>]
    member __.``1. Parallel : exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            cloud {
                let! counter = CloudAtom.New 0
                let worker i = cloud { 
                    if i = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep delayFactor
                        do! CloudAtom.Incr counter |> Local.Ignore
                }

                try
                    let! _ = Array.init 20 worker |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    return! counter.Value
            } |> run |> Choice.shouldEqual 0)

    [<Test>]
    member __.``1. Parallel : nested exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            cloud {
                let! counter = CloudAtom.New 0
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep delayFactor
                        do! CloudAtom.Incr counter |> Local.Ignore
                }

                let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
                try
                    do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    do! Cloud.Sleep delayFactor
                    return! counter.Value

            } |> run |> Choice.shouldEqual 0)
            

    [<Test>]
    member __.``1. Parallel : simple cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let counter = CloudAtom.New 0 |> runLocally
            runCts(fun cts -> cloud {
                let f i = cloud {
                    if i = 0 then cts.Cancel() 
                    do! Cloud.Sleep delayFactor
                    do! CloudAtom.Incr counter |> Local.Ignore
                }

                let! _ = Array.init 10 f |> Cloud.Parallel

                return ()
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

            counter.Value |> runLocally |> shouldEqual 0)

    [<Test>]
    member __.``1. Parallel : as local`` () =
        repeat(fun () ->
            // check local semantics are forced by using ref cells.
            local {
                let counter = ref 0
                let seqWorker _ = cloud {
                    do! Cloud.Sleep 10
                    Interlocked.Increment counter |> ignore
                }

                let! results = Array.init 100 seqWorker |> Cloud.Parallel |> Cloud.AsLocal
                return counter.Value
            } |> run |> Choice.shouldEqual 100)

    [<Test>]
    member __.``1. Parallel : local`` () =
        repeat(fun () ->
            // check local semantics are forced by using ref cells.
            local {
                let counter = ref 0
                let seqWorker _ = local {
                    do! Cloud.Sleep 10
                    Interlocked.Increment counter |> ignore
                }

                let! results = Array.init 100 seqWorker |> Local.Parallel
                return counter.Value
            } |> run |> Choice.shouldEqual 100)

    [<Test>]
    member __.``1. Parallel : MapReduce recursive`` () =
        // naive, binary recursive mapreduce implementation
        repeat(fun () -> WordCount.run 20 WordCount.mapReduceRec |> run |> Choice.shouldEqual 100)

    [<Test>]
    member __.``1. Parallel : MapReduce balanced`` () =
        // balanced, core implemented MapReduce algorithm
        repeat(fun () -> WordCount.run 1000 Cloud.Balanced.mapReduceLocal |> run |> Choice.shouldEqual 5000)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.map`` () =
        let checker (ints : int list) =
            let expected = ints |> List.map (fun i -> i + 1) |> List.toArray
            ints
            |> Cloud.Balanced.mapLocal (fun i -> local { return i + 1})
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.filter`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 5 = 0 || i % 7 = 0) |> List.toArray
            ints
            |> Cloud.Balanced.filterLocal (fun i -> local { return i % 5 = 0 || i % 7 = 0 })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.choose`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 5 = 0 || i % 7 = 0 then Some i else None) |> List.toArray
            ints
            |> Cloud.Balanced.chooseLocal (fun i -> local { return if i % 5 = 0 || i % 7 = 0 then Some i else None })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.fold`` () =
        let checker (ints : int list) =
            let expected = ints |> List.fold (fun s i -> s + i) 0
            ints
            |> Cloud.Balanced.fold (fun s i -> s + i) (fun s i -> s + i) 0
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.collect`` () =
        let checker (ints : int []) =
            let expected = ints |> Array.collect (fun i -> [|(i,1) ; (i,2) ; (i,3)|])
            ints
            |> Cloud.Balanced.collectLocal (fun i -> local { return [(i,1) ; (i,2) ; (i,3)] })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.groupBy`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.toArray v) |> Seq.toArray
            ints
            |> Cloud.Balanced.groupBy id
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.foldBy`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.sum v) |> Seq.toArray
            ints
            |> Cloud.Balanced.foldBy id (+) (+) (fun _ -> 0)
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.foldByLocal`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.sum v) |> Seq.toArray
            ints
            |> Cloud.Balanced.foldByLocal id (fun x y -> local { return x + y }) (fun x y -> local { return x + y }) (fun _ -> local { return 0 })
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``1. Parallel : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! results = Cloud.ParallelEverywhere Cloud.CurrentWorker
                    return set results = set workers
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``1. Parallel : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Parallel [ for i in 1 .. 5 -> (Cloud.CurrentWorker, thisWorker) ]
                    return results |> Array.forall ((=) thisWorker)
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``1. Parallel : nonserializable type`` () =
        if __.UsesSerialization then
            cloud { 
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return new System.Net.WebClient() } ]
                return ()
            } |> run |> Choice.shouldFailwith<_, SerializationException>

    [<Test>]
    member __.``1. Parallel : nonserializable object`` () =
        if __.UsesSerialization then
            cloud { 
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return box (new System.Net.WebClient()) } ]
                return ()
            } |> run |> Choice.shouldFailwith<_, SerializationException>

    [<Test>]
    member __.``1. Parallel : nonserializable closure`` () =
        if __.UsesSerialization then
            cloud { 
                let client = new System.Net.WebClient()
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return box client } ]
                return ()
            } |> run |> Choice.shouldFailwith<_, SerializationException>

    //
    //  2. Choice tests
    //

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
            let parallelismFactor = parallelismFactor
            let count = CloudAtom.New 0 |> runLocally
            cloud {
                let worker _ = cloud {
                    let! _ = CloudAtom.Incr count
                    return None
                }

                return! Array.init parallelismFactor worker |> Cloud.Choice
            } |> run |> Choice.shouldEqual None

            count.Value |> runLocally |> shouldEqual parallelismFactor)

    [<Test>]
    member __.``2. Choice : one input 'Some'`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let count = CloudAtom.New 0 |> runLocally
            cloud {
                let worker i = cloud {
                    if i = 0 then return Some i
                    else
                        do! Cloud.Sleep delayFactor
                        // check proper cancellation while we're at it.
                        let! _ = CloudAtom.Incr count
                        return None
                }

                return! Array.init parallelismFactor worker |> Cloud.Choice
            } |> run |> Choice.shouldEqual (Some 0)
            count.Value |> runLocally |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : all inputs 'Some'`` () =
        repeat(fun () ->
            let successcounter = CloudAtom.New 0 |> runLocally
            cloud {
                let worker _ = cloud { return Some 42 }
                let! result = Array.init 100 worker |> Cloud.Choice
                let! _ = CloudAtom.Incr successcounter
                return result
            } |> run |> Choice.shouldEqual (Some 42)

            // ensure only one success continuation call
            successcounter.Value |> runLocally |> shouldEqual 1)

    [<Test>]
    member __.``2. Choice : simple nested`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runLocally
            cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return Some(i,j)
                    else
                        do! Cloud.Sleep delayFactor
                        let! _ = CloudAtom.Incr counter
                        return None
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } |> run |> Choice.shouldEqual (Some(0,0))

            counter.Value |> runLocally |> shouldBe (fun i ->  i < parallelismFactor / 2))

    [<Test>]
    member __.``2. Choice : nested exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runLocally
            cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return invalidOp "failure"
                    else
                        do! Cloud.Sleep delayFactor
                        let! _ = CloudAtom.Incr counter
                        return Some 42
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

            counter.Value |> runLocally |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : simple cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let counter = CloudAtom.New 0 |> runLocally
            runCts(fun cts ->
                cloud {
                    let worker i = cloud {
                        if i = 0 then cts.Cancel()
                        do! Cloud.Sleep delayFactor
                        let! _ = CloudAtom.Incr counter
                        return Some 42
                    }

                    return! Array.init parallelismFactor worker |> Cloud.Choice
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

            counter.Value |> runLocally |> shouldEqual 0)

    [<Test>]
    member __.``2. Choice : as local`` () =
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // check local semantics are forced by using ref cells.
            local {
                let counter = ref 0
                let seqWorker i = cloud {
                    if i = parallelismFactor / 2 then
                        do! Cloud.Sleep 100
                        return Some i
                    else
                        let _ = Interlocked.Increment counter
                        return None
                }

                let! result = Array.init parallelismFactor seqWorker |> Cloud.Choice |> Cloud.AsLocal
                counter.Value |> shouldEqual (parallelismFactor - 1)
                return result
            } |> run |> Choice.shouldEqual (Some (parallelismFactor / 2)))

    [<Test>]
    member __.``2. Choice : local`` () =
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // check local semantics are forced by using ref cells.
            local {
                let counter = ref 0
                let seqWorker i = local {
                    if i = parallelismFactor / 2 then
                        do! Cloud.Sleep 100
                        return Some i
                    else
                        let _ = Interlocked.Increment counter
                        return None
                }

                let! result = Array.init parallelismFactor seqWorker |> Local.Choice
                counter.Value |> shouldEqual (parallelismFactor - 1)
                return result
            } |> run |> Choice.shouldEqual (Some (parallelismFactor / 2)))

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.tryFind`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 7 = 0 && i % 5 = 0) |> set
            ints
            |> Cloud.Balanced.tryFindLocal (fun i -> local { return i % 7 = 0 && i % 5 = 0 })
            |> run
            |> Choice.shouldBe(function None -> Set.isEmpty expected | Some r -> expected.Contains r)

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.tryPick`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 7 = 0 && i % 5 = 0 then Some i else None) |> set
            ints
            |> Cloud.Balanced.tryPickLocal (fun i -> local { return if i % 7 = 0 && i % 5 = 0 then Some i else None })
            |> run
            |> Choice.shouldBe (function None -> Set.isEmpty expected | Some r -> expected.Contains r)

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.exists`` () =
        let checker (bools : bool []) =
            let expected = Array.exists id bools
            bools
            |> Cloud.Balanced.exists id
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.forall`` () =
        let checker (bools : bool []) =
            let expected = Array.forall id bools
            bools
            |> Cloud.Balanced.forall id
            |> run
            |> Choice.shouldEqual expected

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests)

    [<Test>]
    member __.``2. Choice : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! counter = CloudAtom.New 0
                    let! _ = Cloud.ChoiceEverywhere (cloud { let! _ = CloudAtom.Incr counter in return Option<int>.None })
                    let! value = counter.Value
                    return value = workers.Length
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``2. Choice : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Choice [ for i in 1 .. 5 -> (cloud { let! w = Cloud.CurrentWorker in return Some w }, thisWorker)]
                    return results.Value = thisWorker
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``2. Choice : nonserializable closure`` () =
        if __.UsesSerialization then
            cloud { 
                let client = new System.Net.WebClient()
                let! _ = Cloud.Choice [ for i in 1 .. 5 -> cloud { return Some (box client) } ]
                return ()
            } |> run |> Choice.shouldFailwith<_, SerializationException>



    //
    //  3. Task tests
    //

    [<Test>]
    member __.``3. Task: task with success`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            cloud {
                use! count = CloudAtom.New 0
                let tworkflow = cloud {
                    do! Cloud.Sleep delayFactor
                    let! _ = CloudAtom.Incr count
                    return! count.Value
                }

                let! task = Cloud.StartAsTask(tworkflow)
                let! value = count.Value
                value |> shouldEqual 0
                return! Cloud.AwaitCloudTask task
            } |> run |> Choice.shouldEqual 1)

    [<Test>]
    member __.``3. Task: task with exception`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runLocally
            cloud {
                let tworkflow = cloud {
                    do! Cloud.Sleep delayFactor
                    let! _ = CloudAtom.Incr count
                    return invalidOp "failure"
                }

                let! task = Cloud.StartAsTask(tworkflow)
                let! value = count.Value
                value |> shouldEqual 0
                do! Cloud.Sleep (delayFactor / 10)
                // ensure no exception is raised in parent workflow
                // before the child workflow is properly evaluated
                let! _ = CloudAtom.Incr count
                return! Cloud.AwaitCloudTask task
            } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

            count.Value |> runLocally |> shouldEqual 2)

    [<Test>]
    member __.``3. Task: with cancellation token`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runLocally
            cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let tworkflow = cloud {
                    let! _ = CloudAtom.Incr count
                    do! Cloud.Sleep delayFactor
                    do! CloudAtom.Incr count |> Local.Ignore
                }
                let! task = Cloud.StartAsTask(tworkflow, cancellationToken = cts.Token)
                do! Cloud.Sleep (delayFactor / 3)
                let! value = count.Value
                value |> shouldEqual 1
                cts.Cancel()
                return! Cloud.AwaitCloudTask task
            } |> run |> Choice.shouldFailwith<_, OperationCanceledException>
            
            // ensure final increment was cancelled.
            count.Value |> runLocally |> shouldEqual 1)

    [<Test>]
    member __.``3. Task: to current worker`` () =
        let delayFactor = delayFactor
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! currentWorker = Cloud.CurrentWorker
                    let! task = Cloud.StartAsTask(Cloud.CurrentWorker, target = currentWorker)
                    let! result = Cloud.AwaitCloudTask task
                    return result = currentWorker
                } |> run |> Choice.shouldEqual true)

    [<Test>]
    member __.``3. Task: await with timeout`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            cloud {
                let! task = Cloud.StartAsTask(Cloud.Sleep delayFactor)
                return! Cloud.AwaitCloudTask(task, timeoutMilliseconds = 1)
            } |> run |> Choice.shouldFailwith<_, TimeoutException>)

    [<Test>]
    member __.``1. Task : nonserializable type`` () =
        if __.UsesSerialization then
            cloud { return new System.Net.WebClient() }
            |> run |> Choice.shouldFailwith<_, SerializationException>

    [<Test>]
    member __.``1. Task : nonserializable object`` () =
        if __.UsesSerialization then
            cloud { return box (new System.Net.WebClient()) }
            |> run |> Choice.shouldFailwith<_, SerializationException>

    [<Test>]
    member __.``1. Task : nonserializable closure`` () =
        if __.UsesSerialization then
            cloud { 
                let client = new System.Net.WebClient()
                return! Cloud.StartAsTask(cloud { return box client })

            } |> run |> Choice.shouldFailwith<_, SerializationException>


    //
    //  4. Misc tests
    //
        

    [<Test>]
    member t.``4. Logging`` () =
        let delayFactor = delayFactor
        t.Logs.Clear()
        cloud {
            let logSeq _ = cloud {
                for i in [1 .. 100] do
                    do! Cloud.Logf "user cloud message %d" i
            }

            do! Seq.init 20 logSeq |> Cloud.Parallel |> Cloud.Ignore
            do! Cloud.Sleep delayFactor
        } |> t.Run |> ignore
        
        t.Logs.GetLogs() 
        |> Seq.filter (fun m -> m.Contains "user cloud message") 
        |> Seq.length 
        |> shouldEqual 2000

    [<Test>]
    member __.``4. IsTargetWorkerSupported`` () =
        Cloud.IsTargetedWorkerSupported |> run |> Choice.shouldEqual __.IsTargetWorkerSupported

    [<Test>]
    member __.``4. Cancellation token: simple cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            cts.Token.IsCancellationRequested |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: distributed cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! _ = Cloud.StartAsTask(cloud { cts.Cancel() })
            do! Cloud.Sleep delayFactor
            cts.Token.IsCancellationRequested |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: simple parent cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0 = Cloud.CreateLinkedCancellationTokenSource(cts.Token)
            cts.Token.IsCancellationRequested |> shouldEqual false
            cts0.Token.IsCancellationRequested |> shouldEqual false
            do cts.Cancel()
            do! Cloud.Sleep delayFactor
            cts.Token.IsCancellationRequested |> shouldEqual true
            cts0.Token.IsCancellationRequested |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: simple child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0 = Cloud.CreateLinkedCancellationTokenSource(cts.Token)
            cts.Token.IsCancellationRequested |> shouldEqual false
            cts0.Token.IsCancellationRequested |> shouldEqual false
            do cts0.Cancel()
            do! Cloud.Sleep delayFactor
            cts.Token.IsCancellationRequested |> shouldEqual false
            cts0.Token.IsCancellationRequested |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: distributed child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0, cts1 = Cloud.CreateLinkedCancellationTokenSource() <||> Cloud.CreateLinkedCancellationTokenSource()
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            cts0.Token.IsCancellationRequested |> shouldEqual true
            cts1.Token.IsCancellationRequested |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: nested distributed child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let mkNested () = Cloud.CreateLinkedCancellationTokenSource() <||> Cloud.CreateLinkedCancellationTokenSource()
            let! (cts0, cts1), (cts2, cts3) = mkNested () <||> mkNested ()
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            cts0.Token.IsCancellationRequested |> shouldEqual true
            cts1.Token.IsCancellationRequested |> shouldEqual true
            cts2.Token.IsCancellationRequested |> shouldEqual true
            cts3.Token.IsCancellationRequested |> shouldEqual true

        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Cancellation token: local semantics`` () =
        local {
            let! cts = 
                let cp = Local.Parallel [ Cloud.CancellationToken ; Cloud.CancellationToken ]
                Local.Parallel [cp ; cp]

            cts
            |> Array.concat
            |> Array.forall (fun ct -> ct.IsCancellationRequested)
            |> shouldEqual true
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. Fault Policy: update over parallelism`` () =
        // checks that non-serializable entities do not get accidentally captured in closures.
        cloud {
            let workflow = Cloud.Parallel[Cloud.FaultPolicy ; Cloud.FaultPolicy]
            let! results = Cloud.WithFaultPolicy (FaultPolicy.ExponentialDelay(3)) workflow
            return ()
        } |> run |> Choice.shouldEqual ()

    [<Test>]
    member __.``4. DomainLocal`` () =
        let domainLocal = DomainLocal.Create(local { let! w = Cloud.CurrentWorker in return Guid.NewGuid(), w })
        cloud {
            let! results = Cloud.ParallelEverywhere domainLocal.Value 
            let! results' = Cloud.ParallelEverywhere domainLocal.Value
            return (set results') |> shouldEqual (set results)
        } |> run |> Choice.shouldEqual ()