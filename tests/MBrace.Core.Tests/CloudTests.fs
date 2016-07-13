namespace MBrace.Core.Tests
    
open System
open System.Threading
open System.Runtime.Serialization

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Library.CloudCollectionUtils

#nowarn "444"

/// Suite for testing MBrace parallelism & distribution
/// <param name="parallelismFactor">Maximum permitted parallel work items permitted in tests.</param>
/// <param name="delayFactor">
///     Delay factor in milliseconds used by unit tests. 
///     Use a value that ensures propagation of updates across the cluster.
/// </param>
[<TestFixture>]
[<AbstractClass>]
type ``Cloud Tests`` (parallelismFactor : int, delayFactor : int) as self =

    let nNested = parallelismFactor |> float |> sqrt |> ceil |> int

    let repeat f = repeat self.Repeats f

    let runOnCloud (workflow : Cloud<'T>) = self.Run workflow
    let runOnCloudCts (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = self.Run workflow
    let runOnCloudWithLogs (workflow : Cloud<unit>) = self.RunWithLogs workflow
    let runOnCurrentProcess (workflow : Cloud<'T>) = self.RunLocally workflow
    
    
    static let getRefHashCode (t : 'T when 'T : not struct) = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode t
    
    /// Run workflow in the runtime under test
    abstract Run : workflow:Cloud<'T> -> 'T
    /// Run workflow in the runtime under test, with cancellation token source passed to the worker
    abstract Run : workflow:(ICloudCancellationTokenSource -> #Cloud<'T>) -> 'T
    /// Run workflow in the runtime under test, returning logs created by the process.
    abstract RunWithLogs : workflow:Cloud<unit> -> string []
    /// Evaluate workflow in the local test process
    abstract RunLocally : workflow:Cloud<'T> -> 'T
    /// Maximum number of tests to be run by FsCheck
    abstract FsCheckMaxTests : int
    /// Maximum number of repeats to run nondeterministic tests
    abstract Repeats : int
    /// Enables targeted worker tests
    abstract IsTargetWorkerSupported : bool
    /// Enables object sifting tests
    abstract IsSiftedWorkflowSupported : bool
    /// Declares that this runtime uses serialization/distribution
    abstract UsesSerialization : bool

    //
    //  1. Parallelism tests
    //

    [<Test>]
    member __.``1. Parallel : empty input`` () =
        test <@ Array.empty<Cloud<int>> |> Cloud.Parallel |> runOnCloud = [||] @>

    [<Test>]
    member __.``1. Parallel : simple inputs`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let f i = cloud { return i + 1 }
            let! results = Seq.init parallelismFactor f |> Cloud.Parallel
            return test <@ Array.sum results = (Seq.init parallelismFactor (fun i -> i + 1) |> Seq.sum) @>
        } |> runOnCloud

    [<Test>]
    member __.``1. Parallel : items should preserve order`` () =
        let inputs = [|1 .. 10|]
        let result =
            inputs
            |> Seq.map (fun i -> cloud { return i })
            |> Cloud.Parallel
            |> runOnCloud

        test <@ inputs = result @>
        
    [<Test>]
    member __.``1. Parallel : random inputs`` () =
        let checker (ints:int[]) =
            if ints = null then () else
            let maxSize = 5 * parallelismFactor
            let ints = if ints.Length <= maxSize then ints else ints.[..maxSize]
            cloud {
                let f i = cloud { return ints.[i] }
                let! result = Seq.init ints.Length f |> Cloud.Parallel
                test <@ result = ints @>
            } |> runOnCloud

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : use binding`` () =
        let parallelismFactor = parallelismFactor
        let c = CloudAtom.New 0 |> runOnCurrentProcess
        let comp = cloud {
            use foo = { new ICloudDisposable with member __.Dispose () = c.TransactAsync(fun i -> (), i + 1) }
            let! _ = Seq.init parallelismFactor (fun _ -> CloudAtom.Increment c) |> Cloud.Parallel
            return! c.GetValueAsync()
        } 
        
        test <@ runOnCloud comp = parallelismFactor @>
        test <@ c.Value = parallelismFactor + 1 @>

    [<Test>]
    member  __.``1. Parallel : exception handler`` () =
        let comp = cloud {
            try
                let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
                return x + y
            with :? InvalidOperationException as e ->
                let! x,y = cloud { return 1 } <||> cloud { return 2 }
                return x + y
        } 
        
        test <@ runOnCloud comp = 3 @>

    [<Test>]
    member  __.``1. Parallel : finally`` () =
        let trigger = runOnCurrentProcess <| CloudAtom.New 0
        let comp = Cloud.TryFinally( cloud {
            let! x,y = cloud { return 1 } <||> cloud { return invalidOp "failure" }
            return () }, CloudAtom.Increment trigger |> Local.Ignore)

        raises<InvalidOperationException> <@ runOnCloud comp @>
        test <@ trigger.Value = 1 @>

    [<Test>]
    member __.``1. Parallel : simple nested`` () =
        let nNested = nNested
        let comp = cloud {
            let f i j = cloud { return i + j + 1 }
            let cluster i = Array.init nNested (f i) |> Cloud.Parallel
            let! results = Array.init nNested cluster |> Cloud.Parallel
            return Array.concat results |> Array.sum
        } 

        let expected = 
            Seq.init nNested (fun i -> Seq.init nNested (fun j -> i + j + 1)) 
            |> Seq.concat 
            |> Seq.sum
        
        test <@ runOnCloud comp = expected @>
            
    [<Test>]
    member __.``1. Parallel : simple exception`` () =
        let parallelismFactor = parallelismFactor
        let comp = cloud {
            let f i = cloud { return if i = parallelismFactor / 2 then invalidOp "failure" else i + 1 }
            let! results = Array.init parallelismFactor f |> Cloud.Parallel
            return Array.sum results
        } 
        
        raises<InvalidOperationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``1. Parallel : exception contention`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // test that exception continuation was fired precisely once
            let comp = cloud {
                let! atom = CloudAtom.New 0
                try                    
                    let! _ = Array.init parallelismFactor (fun _ -> cloud { return invalidOp "failure" }) |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    let! _ = CloudAtom.Increment atom
                    return ()

                do! Cloud.Sleep 500
                return! atom.GetValueAsync()
            } 
            
            test <@ runOnCloud comp = 1 @>)

    [<Test>]
    member __.``1. Parallel : exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let comp = cloud {
                let! counter = CloudAtom.New 0
                let worker i = cloud { 
                    if i = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep delayFactor
                        do! CloudAtom.Increment counter |> Local.Ignore
                }

                try
                    let! _ = Array.init 20 worker |> Cloud.Parallel
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    return! counter.GetValueAsync()
            } 
            
            test <@ runOnCloud comp = 0 @>)

    [<Test>]
    member __.``1. Parallel : nested exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let comp = cloud {
                let! counter = CloudAtom.New 0
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        invalidOp "failure"
                    else
                        do! Cloud.Sleep delayFactor
                        do! CloudAtom.Increment counter |> Local.Ignore
                }

                let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
                try
                    do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
                    return raise <| new AssertionException("Cloud.Parallel should not have completed succesfully.")
                with :? InvalidOperationException ->
                    do! Cloud.Sleep delayFactor
                    return! counter.GetValueAsync()

            } 
            
            test <@ runOnCloud comp = 0 @>)
            

    [<Test>]
    member __.``1. Parallel : simple cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let counter = CloudAtom.New 0 |> runOnCurrentProcess
            let run (cts : ICloudCancellationTokenSource) = cloud {
                let f i = cloud {
                    if i = 0 then cts.Cancel() 
                    do! Cloud.Sleep delayFactor
                    do! CloudAtom.Increment counter |> Local.Ignore
                }

                let! _ = Array.init 10 f |> Cloud.Parallel

                return ()
            } 
            
            raises<OperationCanceledException> <@ runOnCloudCts run @>
            test <@ counter.Value = 0 @>)

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
                return test <@ counter.Value = 100 @>
            } |> runOnCloud)

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
                return test <@ counter.Value = 100 @>
            } |> runOnCloud)

    [<Test>]
    member __.``1. Parallel : MapReduce recursive`` () =
        // naive, binary recursive mapreduce implementation
        repeat(fun () -> 
            let result = WordCount.run 20 WordCount.mapReduceRec |> runOnCloud
            test <@ result = 100 @>)

    [<Test>]
    member __.``1. Parallel : MapReduce balanced`` () =
        // balanced, core implemented MapReduce algorithm
        repeat(fun () -> 
            let result = WordCount.run 1000 Cloud.Balanced.mapReduceLocal |> runOnCloud
            test <@ result = 5000 @>)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.map`` () =
        let checker (ints : int list) =
            let expected = ints |> List.map (fun i -> i + 1) |> List.toArray
            let actual =
                ints
                |> Cloud.Balanced.mapLocal (fun i -> local { return i + 1})
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.filter`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 5 = 0 || i % 7 = 0) |> List.toArray
            let actual =
                ints
                |> Cloud.Balanced.filterLocal (fun i -> local { return i % 5 = 0 || i % 7 = 0 })
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.choose`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 5 = 0 || i % 7 = 0 then Some i else None) |> List.toArray
            let actual =
                ints
                |> Cloud.Balanced.chooseLocal (fun i -> local { return if i % 5 = 0 || i % 7 = 0 then Some i else None })
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.fold`` () =
        let checker (ints : int list) =
            let expected = ints |> List.fold (fun s i -> s + i) 0
            let actual =
                ints
                |> Cloud.Balanced.fold (fun s i -> s + i) (fun s i -> s + i) 0
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.collect`` () =
        let checker (ints : int []) =
            let expected = ints |> Array.collect (fun i -> [|(i,1) ; (i,2) ; (i,3)|])
            let actual =
                ints
                |> Cloud.Balanced.collectLocal (fun i -> local { return [(i,1) ; (i,2) ; (i,3)] })
                |> runOnCloud
            
            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.groupBy`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.toArray v) |> Seq.toArray
            let actual =
                ints
                |> Cloud.Balanced.groupBy id
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.foldBy`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.sum v) |> Seq.toArray
            let actual =
                ints
                |> Cloud.Balanced.foldBy id (+) (+) (fun _ -> 0)
                |> runOnCloud
            
            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : Cloud.Balanced.foldByLocal`` () =
        let checker (ints : int []) =
            let expected = ints |> Seq.groupBy id |> Seq.map (fun (k,v) -> k, Seq.sum v) |> Seq.toArray
            let actual =
                ints
                |> Cloud.Balanced.foldByLocal id (fun x y -> local { return x + y }) (fun x y -> local { return x + y }) (fun _ -> local { return 0 })
                |> runOnCloud
            
            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``1. Parallel : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! results = Cloud.ParallelEverywhere Cloud.CurrentWorker
                    return test <@ set results = set workers @>
                } |> runOnCloud)

    [<Test>]
    member __.``1. Parallel : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Parallel [ for i in 1 .. 20 -> (Cloud.CurrentWorker, thisWorker) ]
                    return test <@ results |> Array.forall ((=) thisWorker) @>
                } |> runOnCloud)

    [<Test>]
    member __.``1. Parallel : nonserializable type`` () =
        if __.UsesSerialization then
            let comp = cloud { 
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return new System.Net.WebClient() } ]
                return ()
            } 

            raises<SerializationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``1. Parallel : nonserializable object`` () =
        if __.UsesSerialization then
            let comp = cloud { 
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return box (new System.Net.WebClient()) } ]
                return ()
            }

            raises<SerializationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``1. Parallel : nonserializable closure`` () =
        if __.UsesSerialization then
            let comp = cloud { 
                let client = new System.Net.WebClient()
                let! _ = Cloud.Parallel [ for i in 1 .. 5 -> cloud { return box client } ]
                return ()
            }

            raises<SerializationException> <@ runOnCloud comp @>

    //
    //  2. Choice tests
    //

    [<Test>]
    member __.``2. Choice : empty input`` () =
        test <@ Cloud.Choice List.empty<Cloud<int option>> |> runOnCloud = None @>

    [<Test>]
    member __.``2. Choice : random inputs`` () =
        let checker (size : bool list) =
            let expected = size |> Seq.mapi (fun i b -> (i,b)) |> Seq.filter snd |> Seq.map fst |> set
            let worker i b = cloud { return if b then Some i else None }
            let actual =
                size 
                |> Seq.mapi worker 
                |> Cloud.Choice
                |> runOnCloud 

            test <@ match actual with Some r -> expected.Contains r | None -> Set.isEmpty expected @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``2. Choice : all inputs 'None'`` () =
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let count = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let worker _ = cloud {
                    let! _ = CloudAtom.Increment count
                    return None
                }

                return! Array.init parallelismFactor worker |> Cloud.Choice
            } 
            
            test <@ runOnCloud comp = None @>
            test <@ count.Value = parallelismFactor @>)

    [<Test>]
    member __.``2. Choice : one input 'Some'`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let count = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let worker i = cloud {
                    if i = 0 then return Some i
                    else
                        do! Cloud.Sleep delayFactor
                        // check proper cancellation while we're at it.
                        let! _ = CloudAtom.Increment count
                        return None
                }

                return! Array.init parallelismFactor worker |> Cloud.Choice
            } 
            
            test <@ runOnCloud comp = Some 0 @>
            test <@ count.Value = 0 @>)

    [<Test>]
    member __.``2. Choice : all inputs 'Some'`` () =
        repeat(fun () ->
            let successcounter = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let worker _ = cloud { return Some 42 }
                let! result = Array.init 100 worker |> Cloud.Choice
                let! _ = CloudAtom.Increment successcounter
                return result
            } 
            
            test <@ runOnCloud comp = Some 42 @>
            // ensure only one success continuation call
            test <@ successcounter.Value = 1 @>)

    [<Test>]
    member __.``2. Choice : simple nested`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return Some(i,j)
                    else
                        do! Cloud.Sleep delayFactor
                        let! _ = CloudAtom.Increment counter
                        return None
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } 
            
            test <@ runOnCloud comp = Some(0,0) @>
            test <@ counter.Value < parallelismFactor / 2 @>)

    [<Test>]
    member __.``2. Choice : nested exception cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let nNested = nNested
            let counter = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let worker i j = cloud {
                    if i = 0 && j = 0 then
                        return invalidOp "failure"
                    else
                        do! Cloud.Sleep (2 * delayFactor)
                        let! _ = CloudAtom.Increment counter
                        return Some 42
                }

                let cluster i = Array.init nNested (worker i) |> Cloud.Choice
                return! Array.init nNested cluster |> Cloud.Choice
            } 
            
            raises<InvalidOperationException> <@ runOnCloud comp @>
            test <@ counter.Value = 0 @>)

    [<Test>]
    member __.``2. Choice : simple cancellation`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            let counter = CloudAtom.New 0 |> runOnCurrentProcess
            let mkComp (cts:ICloudCancellationTokenSource) = cloud {
                let worker i = cloud {
                    if i = 0 then cts.Cancel()
                    do! Cloud.Sleep delayFactor
                    let! _ = CloudAtom.Increment counter
                    return Some 42
                }

                return! Array.init parallelismFactor worker |> Cloud.Choice
            } 
            
            raises<OperationCanceledException> <@ runOnCloudCts mkComp @>
            test <@ counter.Value = 0 @>)

    [<Test>]
    member __.``2. Choice : as local`` () =
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // check local semantics are forced by using ref cells.
            let comp = local {
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
                test <@ counter.Value = (parallelismFactor - 1) @>
                return result
            } 
            
            test <@ runOnCloud comp = Some (parallelismFactor / 2) @>)

    [<Test>]
    member __.``2. Choice : local`` () =
        repeat(fun () ->
            let parallelismFactor = parallelismFactor
            // check local semantics are forced by using ref cells.
            let comp = local {
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
                test <@ counter.Value = parallelismFactor - 1 @>
                return result
            } 
            
            test <@ runOnCloud comp = Some (parallelismFactor / 2) @>)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.tryFind`` () =
        let checker (ints : int list) =
            let expected = ints |> List.filter (fun i -> i % 7 = 0 && i % 5 = 0) |> set
            let actual =
                ints
                |> Cloud.Balanced.tryFindLocal (fun i -> local { return i % 7 = 0 && i % 5 = 0 })
                |> runOnCloud

            test <@ match actual with None -> Set.isEmpty expected | Some r -> expected.Contains r @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.tryPick`` () =
        let checker (ints : int list) =
            let expected = ints |> List.choose (fun i -> if i % 7 = 0 && i % 5 = 0 then Some i else None) |> set
            let actual =
                ints
                |> Cloud.Balanced.tryPickLocal (fun i -> local { return if i % 7 = 0 && i % 5 = 0 then Some i else None })
                |> runOnCloud

            test <@ match actual with None -> Set.isEmpty expected | Some r -> expected.Contains r @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.exists`` () =
        let checker (bools : bool []) =
            let expected = Array.exists id bools
            let actual =
                bools
                |> Cloud.Balanced.exists id
                |> runOnCloud

            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``2. Choice : Cloud.Balanced.forall`` () =
        let checker (bools : bool []) =
            let expected = Array.forall id bools
            let actual =
                bools
                |> Cloud.Balanced.forall id
                |> runOnCloud
            
            test <@ expected = actual @>

        Check.QuickThrowOnFail(checker, maxRuns = __.FsCheckMaxTests, shrink = false)

    [<Test>]
    member __.``2. Choice : to all workers`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! workers = Cloud.GetAvailableWorkers()
                    let! counter = CloudAtom.New 0
                    let! _ = Cloud.ChoiceEverywhere (cloud { let! _ = CloudAtom.Increment counter in return Option<int>.None })
                    let! value = counter.GetValueAsync()
                    return test <@ value = workers.Length @>
                } |> runOnCloud)

    [<Test>]
    member __.``2. Choice : to current worker`` () =
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! thisWorker = Cloud.CurrentWorker
                    let! results = Cloud.Choice [ for i in 1 .. 5 -> (cloud { let! w = Cloud.CurrentWorker in return Some w }, thisWorker)]
                    return test <@ results.Value = thisWorker @>
                } |> runOnCloud)

    [<Test>]
    member __.``2. Choice : nonserializable closure`` () =
        if __.UsesSerialization then
            let comp = cloud { 
                let client = new System.Net.WebClient()
                let! _ = Cloud.Choice [ for i in 1 .. 5 -> cloud { return Some (box client) } ]
                return ()
            } 
            
            raises<SerializationException> <@ runOnCloud comp @>



    //
    //  3. Task tests
    //

    [<Test>]
    member __.``3. CloudProcess: with success`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let comp = cloud {
                use! count = CloudAtom.New 0
                let tworkflow = cloud {
                    do! Cloud.Sleep delayFactor
                    let! _ = CloudAtom.Increment count
                    return! count.GetValueAsync()
                }

                let! cloudProcess = Cloud.CreateProcess(tworkflow)
                let! value = count.GetValueAsync()
                test <@ value = 0 @>
                return! Cloud.AwaitProcess cloudProcess
            } 
            
            test <@ runOnCloud comp = 1 @>)

    [<Test>]
    member __.``3. CloudProcess: with exception`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let tworkflow = cloud {
                    do! Cloud.Sleep delayFactor
                    let! _ = CloudAtom.Increment count
                    return invalidOp "failure"
                }

                let! cloudProcess = Cloud.CreateProcess(tworkflow)
                let! value = count.GetValueAsync()
                test <@ value = 0 @>
                do! Cloud.Sleep (delayFactor / 10)
                // ensure no exception is raised in parent workflow
                // before the child workflow is properly evaluated
                let! _ = CloudAtom.Increment count
                return! Cloud.AwaitProcess cloudProcess
            } 
            
            raises<InvalidOperationException> <@ runOnCloud comp @>
            test <@ count.Value = 2 @>)

    [<Test>]
    member __.``3. CloudProcess: with cancellation token`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let count = CloudAtom.New 0 |> runOnCurrentProcess
            let comp = cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let tworkflow = cloud {
                    let! _ = CloudAtom.Increment count
                    do! Cloud.Sleep delayFactor
                    do! CloudAtom.Increment count |> Local.Ignore
                }
                let! job = Cloud.CreateProcess(tworkflow, cancellationToken = cts.Token)
                do! Cloud.Sleep (delayFactor / 3)
                let! value = count.GetValueAsync()
                test <@ value = 1 @>
                cts.Cancel()
                return! Cloud.AwaitProcess job
            } 
            
            raises<OperationCanceledException> <@ runOnCloud comp @>
            // ensure final increment was cancelled.
            test <@ count.Value = 1 @>)

    [<Test>]
    member __.``3. CloudProcess: to current worker`` () =
        let delayFactor = delayFactor
        if __.IsTargetWorkerSupported then
            repeat(fun () ->
                cloud {
                    let! currentWorker = Cloud.CurrentWorker
                    let! job = Cloud.CreateProcess(Cloud.CurrentWorker, target = currentWorker)
                    let! result = Cloud.AwaitProcess job
                    return test <@ result = currentWorker @>
                } |> runOnCloud)

    [<Test>]
    member __.``3. CloudProcess: await with timeout`` () =
        let delayFactor = delayFactor
        repeat(fun () ->
            let comp = cloud {
                let! job = Cloud.CreateProcess(Cloud.Sleep (20 * delayFactor))
                return! Cloud.AwaitProcess(job, timeoutMilliseconds = 1)
            } 
            
            raises<TimeoutException> <@ runOnCloud comp @>)

    [<Test>]
    member __.``3. CloudProcess: await cancelled process`` () =
        let comp = cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            cts.Cancel()
            let! proc = Cloud.CreateProcess(Cloud.Sleep 5000, cancellationToken = cts.Token)
            do! proc.WaitAsync()
            try 
                let! _ = proc.AwaitResult()
                return false
            with :? OperationCanceledException ->
                return true
        } 
        
        test <@ runOnCloud comp = true @>

    [<Test>]
    member __.``1. CloudProcess : nonserializable type`` () =
        if __.UsesSerialization then
            let comp = cloud { return new System.Net.WebClient() }
            raises<SerializationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``1. CloudProcess : nonserializable object`` () =
        if __.UsesSerialization then
            let comp = cloud { return box (new System.Net.WebClient()) }
            raises<SerializationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``1. CloudProcess : nonserializable closure`` () =
        if __.UsesSerialization then
            let comp = cloud { 
                let client = new System.Net.WebClient()
                return! Cloud.CreateProcess(cloud { return box client })
            } 
            
            raises<SerializationException> <@ runOnCloud comp @>

    [<Test>]
    member __.``3. CloudProcess: WhenAny`` () =
        let delayFactor = delayFactor
        let comp = cloud {
            let! ct = Cloud.CancellationToken
            let mkSleeper n = cloud { let! _ = Cloud.Sleep (n * delayFactor) in return n }
            let! procA = Cloud.CreateProcess(mkSleeper 1, cancellationToken = ct)
            let! procB = Cloud.CreateProcess(mkSleeper 2, cancellationToken = ct)
            let! procC = Cloud.CreateProcess(mkSleeper 3, cancellationToken = ct)
            let! result = Cloud.WhenAny(procA, procB, procC)
            return result.Result, procA.IsCompleted, procB.IsCompleted, procC.IsCompleted
        } 
        
        test <@ runOnCloud comp = (1, true, false, false) @>

    [<Test>]
    member __.``3. CloudProcess: WhenAll`` () =
        let delayFactor = delayFactor
        let comp = cloud {
            let! ct = Cloud.CancellationToken
            let mkSleeper n = cloud { let! _ = Cloud.Sleep (n * delayFactor) in return n }
            let! procA = Cloud.CreateProcess(mkSleeper 1, cancellationToken = ct)
            let! procB = Cloud.CreateProcess(mkSleeper 2, cancellationToken = ct)
            let! procC = Cloud.CreateProcess(mkSleeper 3, cancellationToken = ct)
            do! Cloud.WhenAll(procA, procB, procC)
            return [|procA ; procB ; procC|] |> Array.forall (fun p -> p.IsCompleted)
        } 
        
        test <@ runOnCloud comp = true @>

    //
    //  4. Misc tests
    //
        

    [<Test>]
    member t.``4. Logging`` () =
        let delayFactor = delayFactor

        let comp = cloud {
            let logSeq _ = cloud {
                for i in [1 .. 100] do
                    do! Cloud.Logf "user cloud message %d" i
            }

            do! Seq.init 20 logSeq |> Cloud.Parallel |> Cloud.Ignore
            do! Cloud.Sleep delayFactor
        } 

        let logSize = 
            comp
            |> runOnCloudWithLogs
            |> Seq.filter (fun m -> m.Contains "user cloud message") 
            |> Seq.length 

        test <@ logSize = 2000 @>

    [<Test>]
    member t.``4. Clone Object`` () =
        cloud {
            let a = [1 .. 100]
            let! b = Serializer.Clone a
            return test <@ b = a @>
        } |> runOnCloud

    [<Test>]
    member t.``4. Binary Serialize Object`` () =
        cloud {
            let a = [1 .. 100]
            let! bytes = Serializer.Pickle a
            let! b = Serializer.UnPickle<int list> bytes
            return test <@ b = a @>
        } |> runOnCloud

    [<Test>]
    member t.``4. Text Serialize Object`` () =
        cloud {
            let a = [1 .. 100]
            let! text = Serializer.PickleToString a
            let! b = Serializer.UnPickleOfString<int list> text
            return test <@ b = a @>
        } |> runOnCloud

    [<Test>]
    member __.``4. IsTargetWorkerSupported`` () =
        test <@ Cloud.IsTargetedWorkerSupported |> runOnCloud = __.IsTargetWorkerSupported @>

    [<Test>]
    member __.``4. Cancellation token: simple cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            test <@ cts.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: distributed cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! _ = Cloud.CreateProcess(cloud { cts.Cancel() })
            do! Cloud.Sleep delayFactor
            test <@ cts.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: simple parent cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0 = Cloud.CreateCancellationTokenSource(cts.Token)
            test <@ cts.Token.IsCancellationRequested = false @>
            test <@ cts0.Token.IsCancellationRequested = false @>
            do cts.Cancel()
            do! Cloud.Sleep delayFactor
            test <@ cts.Token.IsCancellationRequested = true @>
            test <@ cts0.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: simple child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0 = Cloud.CreateCancellationTokenSource(cts.Token)
            test <@ cts.Token.IsCancellationRequested = false @>
            test <@ cts0.Token.IsCancellationRequested = false @>
            do cts0.Cancel()
            do! Cloud.Sleep delayFactor
            test <@ cts.Token.IsCancellationRequested = false @>
            test <@ cts0.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: distributed child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let! cts0, cts1 = Cloud.CreateCancellationTokenSource(cts.Token) <||> Cloud.CreateCancellationTokenSource(cts.Token)
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            test <@ cts0.Token.IsCancellationRequested = true @>
            test <@ cts1.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: nested distributed child cancellation`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = Cloud.CreateCancellationTokenSource()
            let mkNested () = Cloud.CreateCancellationTokenSource(cts.Token) <||> Cloud.CreateCancellationTokenSource(cts.Token)
            let! (cts0, cts1), (cts2, cts3) = mkNested () <||> mkNested ()
            cts.Cancel()
            do! Cloud.Sleep delayFactor
            test <@ [cts0; cts1; cts2; cts3 ] |> List.forall (fun cts -> cts.Token.IsCancellationRequested) @>

        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: local semantics`` () =
        local {
            let! cts = 
                let cp = Local.Parallel [ Cloud.CancellationToken ; Cloud.CancellationToken ]
                Local.Parallel [cp ; cp]

            test 
                <@
                    cts
                    |> Array.concat
                    |> Array.forall (fun ct -> ct.IsCancellationRequested)
                @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Cancellation token: disposal`` () =
        let delayFactor = delayFactor
        cloud {
            let! cts = cloud {
                use! cts = Cloud.CreateCancellationTokenSource()
                let f () = cloud { test <@ cts.Token.IsCancellationRequested = false @> }
                do! Cloud.Parallel [ f() ; f() ] |> Cloud.Ignore
                test <@ cts.Token.IsCancellationRequested = false @>
                return cts
            }

            do! Cloud.Sleep delayFactor
            test <@ cts.Token.IsCancellationRequested = true @>
        } |> runOnCloud

    [<Test>]
    member __.``4. Fault Policy: update over parallelism`` () =
        // checks that non-serializable entities do not get accidentally captured in closures.
        cloud {
            let workflow = Cloud.Parallel[Cloud.FaultPolicy ; Cloud.FaultPolicy]
            let! results = Cloud.WithFaultPolicy (FaultPolicy.WithExponentialDelay(maxRetries = 3)) workflow
            return ()
        } |> runOnCloud

    [<Test>]
    member __.``4. DomainLocal`` () =
        let domainLocal = DomainLocal.Create(local { let! w = Cloud.CurrentWorker in return Guid.NewGuid(), w })
        cloud {
            let! results = Cloud.ParallelEverywhere domainLocal.Value 
            let! results' = Cloud.ParallelEverywhere domainLocal.Value
            return test <@ set results' = set results @>
        } |> runOnCloud


    [<Test>]
    member __.``5. Simple sifting of large value`` () =
        if __.IsSiftedWorkflowSupported then
            let large = [|1L .. 10000000L|]
            cloud {
                let! workerCount = Cloud.GetWorkerCount()
                // warmup phase; ensure cloudvalue is replicated everywhere before running test
                do! Cloud.ParallelEverywhere(cloud { return getRefHashCode large}) |> Cloud.Ignore
                let! hashCodes = Cloud.Parallel [for i in 1  .. 5 * workerCount -> cloud { return getRefHashCode large } ]
                let length =
                    hashCodes 
                    |> Seq.distinct 
                    |> Seq.length

                test <@ length <= workerCount @>

            } |> runOnCloud

    [<Test>]
    member __.``5. Simple sifting of nested large value`` () =
        if __.IsSiftedWorkflowSupported then
            let large = [|1L .. 10000001L|]
            let smallContainer = [| large ; [||] ; large ; [||] ; large |]
            cloud {
                let! workerCount = Cloud.GetWorkerCount()
                // warmup phase; ensure cloudvalue is replicated everywhere before running test
                do! Cloud.ParallelEverywhere(cloud { return getRefHashCode smallContainer}) |> Cloud.Ignore
                let! hashCodes = Cloud.Parallel [for i in 1  .. 5 * workerCount -> cloud { return getRefHashCode smallContainer, getRefHashCode smallContainer.[0] } ]
                let containerHashCodes, largeHashCodes = Array.unzip hashCodes
                let containerHashCodeCount =
                    containerHashCodes
                    |> Seq.distinct 
                    |> Seq.length
                    
                test <@ containerHashCodeCount > 4 * workerCount @>

                let largeHashCodeCount =
                    largeHashCodes 
                    |> Seq.distinct 
                    |> Seq.length

                test <@ largeHashCodeCount <= workerCount @>

            } |> runOnCloud


    [<Test>]
    member __.``5. Sifting of variably sized large value`` () =
        if __.IsSiftedWorkflowSupported then
            let large = [for i in 1 .. 1000000 -> seq { for j in 0 .. i % 7 -> "lorem ipsum"} |> String.concat "-"]
            cloud {
                let! workerCount = Cloud.GetWorkerCount()
                // warmup phase; ensure cloudvalue is replicated everywhere before running test
                do! Cloud.ParallelEverywhere(cloud { return getRefHashCode large}) |> Cloud.Ignore
                let! hashCodes = Cloud.Parallel [for i in 1  .. 5 * workerCount -> cloud { return getRefHashCode large } ]

                let hashCodeCount =
                    hashCodes 
                    |> Seq.distinct 
                    |> Seq.length

                test <@ hashCodeCount <= workerCount @>

            } |> runOnCloud

    [<Test>]
    member __.``5. Sifting array of intermediately sized elements`` () =
        if __.IsSiftedWorkflowSupported then
            // assumes sift threshold between ~ 500K and 20MB
            let mkSmall() = [|1 .. 100000|]
            let large = [ for i in 1 .. 200 -> mkSmall() ]
            cloud {
                let! workerCount = Cloud.GetWorkerCount()
                // warmup phase; ensure cloudvalue is replicated everywhere before running test
                do! Cloud.ParallelEverywhere(cloud { return getRefHashCode large}) |> Cloud.Ignore
                let! hashCodes = Cloud.Parallel [for i in 1  .. 5 * workerCount -> cloud { return getRefHashCode large } ]

                let hashCodeCount =
                    hashCodes 
                    |> Seq.distinct 
                    |> Seq.length

                test <@ hashCodeCount <= workerCount @>

            } |> runOnCloud