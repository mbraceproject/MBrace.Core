namespace MBrace.Core.Tests

open System
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "443"
#nowarn "444"

/// Core tests for the continuation monad
[<TestFixture>]
[<Category("ContinuationTests")>]
module ``Continuation Tests`` =

    //
    //  Simple expression execution
    //

    let run (wf : Cloud<'T>) = Choice.protect(fun () -> Cloud.RunSynchronously(wf, ResourceRegistry.Empty, new InMemoryCancellationToken()))
    let runCts (wf : ICloudCancellationTokenSource -> #Cloud<'T>) =
        let cts = new InMemoryCancellationTokenSource () :> ICloudCancellationTokenSource
        Choice.protect(fun () -> Cloud.RunSynchronously(wf cts, ResourceRegistry.Empty, cts.Token))

    [<Test>]
    let ``return value`` () =
        cloud { return 1 + 1 } |> run |> Choice.shouldEqual 2

    [<Test>]
    let ``return exception`` () =
        cloud { return 1  / 0 } |> run |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``side effect`` () =
        let cell = ref 0
        let comp = cloud { incr cell } 
        !cell |> shouldEqual 0
        comp |> run |> Choice.shouldEqual ()
        !cell |> shouldEqual 1

    [<Test>]
    let ``uncaught exception`` () =
        cloud { ignore (1 / 0) } |> run |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``let binding`` () =
        cloud { let x = 1 + 1 in return x + x } |> run |> Choice.shouldEqual 4

    [<Test>]
    let ``monadic binding`` () =
        cloud {
            let! x = cloud { return 1 + 1 }
            return x + x
        } |> run |> Choice.shouldEqual 4

    [<Test>]
    let ``exception in nested binding`` () =
        cloud {
            let! x = cloud { return 1 / 0 }
            return x + x
        }  |> run |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``monadic binding with exception on continuation`` () =
        cloud {
            let! x = cloud { return 1 + 1 }
            do invalidOp "failure"
            return x + x
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    let ``combined workflows`` () =
        cloud {
            let cell = ref 0
            do! cloud { incr cell }
            do! cloud { incr cell }
            return !cell
        } |> run |> Choice.shouldEqual 2

    [<Test>]
    let ``exception in nested binding 2`` () =
        cloud {
            let! x = Cloud.Raise (new IndexOutOfRangeException())
            return x + 1
        } |> run |> Choice.shouldFailwith<_, IndexOutOfRangeException>

    [<Test>]
    let ``cancellation`` () =
        let cell = ref 0
        let result =
            runCts(fun cts ->
                cloud {
                    do cts.Cancel()
                    do! cloud { incr cell }
                })

        !cell |> shouldEqual 0
        result |> Choice.shouldFailwith<_, OperationCanceledException>

    [<Test>]
    let ``try with handled exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return None
            with :? IndexOutOfRangeException as e -> return (Some e)
        } |> run |> Choice.shouldBe Option.isSome

    [<Test>]
    let ``try with unhandled exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return false
            with :? DivideByZeroException -> return true
        } |> run |> Choice.shouldFailwith<_, IndexOutOfRangeException>

    [<Test>]
    let ``try with handler raising new exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return false
            with :? IndexOutOfRangeException -> return! Cloud.Raise(new DivideByZeroException())
        } |> run |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``try finally on success`` () =
        let cell = ref 0
        cloud {
            try
                try
                    do raise <| new IndexOutOfRangeException()
                    return false
                with :? IndexOutOfRangeException -> return true
            finally
                incr cell
        } |> run |> Choice.shouldEqual true

        !cell |> shouldEqual 1

    [<Test>]
    let ``try finally on exception`` () =
        let cell = ref 0
        cloud {
            try
                try
                    do raise <| new IndexOutOfRangeException()
                    return false
                with :? DivideByZeroException -> return true
            finally
                incr cell
        } |> run |> Choice.shouldFailwith<_, IndexOutOfRangeException>

        !cell |> shouldEqual 1

    [<Test>]
    let ``try finally on success with exception in finally`` () =
        cloud {
            try
                try
                    do raise <| new IndexOutOfRangeException()
                    return false
                with :? IndexOutOfRangeException -> return true
            finally
                raise <| new InvalidCastException()
        } |> run |> Choice.shouldFailwith<_, InvalidCastException>


    [<Test>]
    let ``try finally on exception with exception in finally`` () =
        cloud {
            try
                try
                    do raise <| new IndexOutOfRangeException()
                    return false
                with :? DivideByZeroException -> return true
            finally
                raise <| new InvalidCastException()
        } |> run |> Choice.shouldFailwith<_, InvalidCastException>

    [<Test>]
    let ``try finally monadic`` () =
        let n = ref 10
        let rec loop () : LocalCloud<unit> =
            Local.TryFinally(
                Cloud.Raise(new Exception()),
                local { if !n > 0 then decr n ; return! loop () }
            )

        loop () |> run |> Choice.shouldFailwith<_, Exception>
        !n |> shouldEqual 0

    [<Test>]
    let ``for loop over array`` () =
        Check.QuickThrowOnFail<int []>(fun (ints : int[]) ->
            if ints = null then () else
            let arr = new ResizeArray<int> ()
            let comp = cloud {
                for i in ints do
                    do! cloud { arr.Add i }
            }
            arr.Count |> shouldEqual 0
            run comp |> Choice.shouldEqual ()
            arr.ToArray() |> shouldEqual ints)

    [<Test>]
    let ``for loop over list`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let arr = new ResizeArray<int> ()
            let comp = cloud {
                for i in ints do
                    do! cloud { arr.Add i }
            }

            arr.Count |> shouldEqual 0
            run comp |> Choice.shouldEqual ()
            arr.ToArray() |> shouldEqual (List.toArray ints))

    [<Test>]
    let ``for loop over sequence`` () =
        Check.QuickThrowOnFail<int []>(fun (ints : int []) ->
            if ints = null then () else
            let dseq = dseq ints
            let arr = new ResizeArray<int> ()
            let comp = cloud {
                for i in dseq do
                    do! cloud { arr.Add i }
            }

            arr.Count |> shouldEqual 0
            run comp |> Choice.shouldEqual ()
            arr.ToArray() |> shouldEqual ints
            dseq.IsDisposed |> shouldEqual true)

    [<Test>]
    let ``for loop on null inputs`` () =
        cloud {
            for i in Unchecked.defaultof<int list> do
                do! cloud { return () }
        } |> run |> Choice.shouldFailwith<_, NullReferenceException>

    [<Test>]
    let ``for loop with exception`` () =
        let cell = ref 0
        cloud {
            for i in 1 .. 100 do
                incr cell
                if i = 55 then return invalidOp "failure"
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>

        !cell |> shouldEqual 55

    [<Test>]
    let ``for loop with cancellation`` () =
        let cell = ref 0
        runCts(fun cts ->
            cloud {
                for i in 1 .. 100 do
                    incr cell
                    if i = 55 then cts.Cancel()
                
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !cell |> shouldEqual 55

    [<Test>]
    let ``while loop`` () =
        cloud {
            let cell = ref 0
            while !cell < 100 do
                incr cell

            return !cell
        } |> run |> Choice.shouldEqual 100

    [<Test>]
    let ``while loop with exception on predicate`` () =
        let cell = ref 0
        cloud {
            while (if !cell < 55 then true else invalidOp "failure") do
                incr cell
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>
        !cell |> shouldEqual 55

    [<Test>]
    let ``while loop with exception on body`` () =
        let cell = ref 0
        cloud {
            while true do
                incr cell
                if !cell = 55 then invalidOp "failure"
        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>
        !cell |> shouldEqual 55

    [<Test>]
    let ``while loop with cancellation`` () =
        let cell = ref 0
        runCts(fun cts ->
            cloud {
                while true do
                    incr cell
                    if !cell = 55 then cts.Cancel()
                
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !cell |> shouldEqual 55

    [<Test>]
    let ``use binding (ICloudDisposable)`` () =
        cloud {
            let! r1, d = cloud {
                use d = new DummyCloudDisposable()
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> run |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use binding (IDisposable)`` () =
        cloud {
            let! r1, d = cloud {
                use d = new DummyIDisposable()
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> run |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use! binding (ICloudDisposable)`` () =
        cloud {
            let! r1, d = cloud {
                use! d = cloud { return new DummyCloudDisposable() }
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> run |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use! binding (IDisposable)`` () =
        cloud {
            let! r1, d = cloud {
                use! d = cloud { return new DummyIDisposable() }
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> run |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use binding (ICloudDisposable) with exception`` () =
        cloud {
            let d = new DummyCloudDisposable ()
            try
                use d = d
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> run |> Choice.shouldEqual true

    [<Test>]
    let ``use binding (IDisposable) with exception`` () =
        cloud {
            let d = new DummyIDisposable ()
            try
                use d = d
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> run |> Choice.shouldEqual true


    [<Test>]
    let ``use! binding (ICloudDisposable) with exception`` () =
        cloud {
            let d = new DummyCloudDisposable ()
            try
                use! d = cloud { return d }
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> run |> Choice.shouldEqual true

    [<Test>]
    let ``use! binding (IDisposable) with exception`` () =
        cloud {
            let d = new DummyIDisposable ()
            try
                use! d = cloud { return d }
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> run |> Choice.shouldEqual true

    //
    //  Advanced tests
    //

    [<Test>]
    let ``factorial`` () =
        let rec fact n =
            if n = 0 then 1
            else
                n * fact(n-1)

        let rec factC n = cloud {
            if n = 0 then return 1
            else
                let! f = factC (n-1)
                return n * f
        }

        for i in 1 .. 10 do
            run (factC i) |> Choice.shouldEqual (fact i)

    [<Test>]
    let ``fibonacci`` () =
        let rec fib n =
            if n <= 1 then n
            else
                fib(n-2) + fib(n-1)

        let rec fibC n = cloud {
            if n <= 1 then return n
            else
                let! f = fibC (n-2)
                let! f' = fibC (n-1)
                return f + f'
        }

        for i in 1 .. 10 do
            run (fibC i) |> Choice.shouldEqual (fib i)

    [<Test>]
    let ``ackermann`` () =
        let rec ackermann m n =
            match m, n with
            | 0, n -> n + 1
            | m, 0 -> ackermann (m-1) 1
            | m, n ->
                ackermann (m-1) (ackermann m (n-1))
            
        let rec ackermannC m n =
            cloud {
                match m, n with
                | 0, n -> return n + 1
                | m, 0 -> return! ackermannC (m-1) 1
                | m, n ->
                    let! right = ackermannC m (n-1)
                    return! ackermannC (m-1) right
            }

        for i in 0 .. 3 do
            run(ackermannC i i) |> Choice.shouldEqual (ackermann i i)

    type N = Z | S of N
    with
        member n.Value =
            let rec aux c = function Z -> c | S p -> aux (c+1) p
            aux 0 n

    [<Test>]
    let ``peano`` () =
        let rec int2Peano n = cloud {
            if n < 0 then return invalidArg "n" "negative peano nums not supported."
            elif n = 0 then return Z
            else
                let! pd = int2Peano (n-1)
                return S pd
        }

        run (int2Peano -1) |> Choice.shouldFailwith<_, ArgumentException>

        for i = 0 to 10 do
            run (cloud { let! p = int2Peano i in return p.Value }) |> Choice.shouldEqual i


    [<Test>]
    let ``stack overflow`` () =
        let rec diveTo n = cloud {
            if n = 0 then return 0
            else
                let! r = diveTo (n-1)
                return 1 + r
        }

        run(diveTo 100000) |> Choice.shouldEqual 100000


    [<Test>]
    let ``async binding stack overflow`` () =
        let rec diveTo n = cloud {
            if n = 0 then return ()
            else
                let! x = Cloud.OfAsync <| async { return n }
                return! diveTo (n-1)
        }

        run(diveTo 100000) |> Choice.shouldEqual ()

    [<Test>]
    let ``deep exception`` () =
        let rec diveRaise n = cloud {
            if n = 0 then return invalidOp "failure"
            else
                let! r = diveRaise (n-1)
                return 1 + r
        }

        run(diveRaise 100000) |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    let ``deep cancellation`` () =
        let rec diveRaise n (cts : ICloudCancellationTokenSource) = cloud {
            if n = 0 then cts.Cancel() ; return 0
            else
                let! r = diveRaise (n-1) cts
                return 1 + r
        }

        runCts(diveRaise 100000) |> Choice.shouldFailwith<_, OperationCanceledException>


    [<Test>]
    let ``finally cancellation`` () =
        let cell = ref false
        let test (cts : ICloudCancellationTokenSource) = cloud {
            try 
                cts.Cancel()
                do! Cloud.Sleep 1000
            finally
                cell := true }

        runCts test |> Choice.shouldFailwith<_, OperationCanceledException>
        !cell |> shouldEqual false

    [<Test>]
    let ``runtime resources`` () =
        run(Cloud.GetWorkerCount()) |> Choice.shouldFailwith<_, ResourceNotFoundException>

    [<Test>]
    let ``storage resouces`` () =
        run(CloudValue.New 0) |> Choice.shouldFailwith<_, ResourceNotFoundException>

    [<Test>]
    let ``test correct scoping in resource updates`` () =
        cloud {
            do! Cloud.WithNestedContext(cloud.Zero(), 
                                    (fun ctx -> { ctx with Resources = ctx.Resources.Register 42 }),
                                    (fun ctx -> { ctx with Resources = ctx.Resources.Remove<int> ()}))

            return! Cloud.TryGetResource<int> ()
        } |> run |> Choice.shouldEqual None


    [<Test>]
    let ``await task`` () =
        let mkTask (t:int) = Tasks.Task.Factory.StartNew(fun () -> Thread.Sleep t ; 42)
        Cloud.AwaitTask (mkTask 0) |> run |> Choice.shouldEqual 42
        Cloud.AwaitTask (mkTask 500) |> run |> Choice.shouldEqual 42

    [<Test>]
    let ``start as task`` () =
        let t = Cloud.StartAsTask(cloud { return 42 }, ResourceRegistry.Empty, new InMemoryCancellationToken())
        t.Result |> shouldEqual 42

    //
    //  Sequential workflow tests
    //

    [<Test>]
    let ``Sequential.map`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.map (fun i -> i + 1) |> List.toArray
            ints 
            |> dseq 
            |> Local.Sequential.map (fun i -> local { return i + 1 }) 
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.filter`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.filter (fun i -> i % 5 = 0 || i % 7 = 0) |> List.toArray
            ints 
            |> dseq 
            |> Local.Sequential.filter (fun i -> local { return i % 5 = 0 || i % 7 = 0 }) 
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.choose`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.choose (fun i -> if i % 5 = 0 then Some i else None) |> List.toArray
            ints 
            |> dseq 
            |> Local.Sequential.choose (fun i -> local { return if i % 5 = 0 then Some i else None }) 
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.fold`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.fold (fun s i -> i + s) 0
            ints 
            |> dseq 
            |> Local.Sequential.fold (fun s i -> local { return s + i }) 0
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.collect`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.collect (fun i -> [(i,1) ; (i,2) ; (i,3)]) |> List.toArray
            ints 
            |> dseq 
            |> Local.Sequential.collect (fun i -> local { return [(i,1) ; (i,2) ; (i,3)] })
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.tryFind`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.tryFind (fun i -> i % 13 = 0 || i % 7 = 0)
            ints 
            |> dseq 
            |> Local.Sequential.tryFind (fun i -> local { return i % 13 = 0 || i % 7 = 0 })
            |> run
            |> Choice.shouldEqual expected)

    [<Test>]
    let ``Sequential.tryPick`` () =
        Check.QuickThrowOnFail<int list>(fun (ints : int list) ->
            let expected = ints |> List.tryPick (fun i -> if i % 13 = 0 || i % 7 = 0 then Some i else None)
            ints 
            |> dseq 
            |> Local.Sequential.tryPick (fun i -> local { return if i % 13 = 0 || i % 7 = 0 then Some i else None })
            |> run
            |> Choice.shouldEqual expected)


    //
    //  Utils tests
    //

    [<Test>]
    let ``Array.splitByChunkSize`` () =
        Check.QuickThrowOnFail<uint16 * uint16>(fun (chunkSize : uint16, arraySize : uint16) ->
            let chunkSize = 1 + int chunkSize // need size > 0
            let arraySize = int arraySize
            if chunkSize > arraySize then () else // expected failure case
            let ts = [|1 .. arraySize|]
            let tss = Array.splitByChunkSize chunkSize ts
            for ch in tss do ch.Length |> shouldBe (fun l -> l <= chunkSize)
            Array.concat tss |> shouldEqual ts)

    [<Test>]
    let ``Array.splitByPartitionCount`` () =
        Check.QuickThrowOnFail<uint16 * uint16>(fun (partitionCount : uint16, arraySize : uint16) ->
            let partitionCount = 1 + int partitionCount // need size > 0
            let arraySize = int arraySize
            let ts = [|1 .. arraySize|]
            let tss = Array.splitByPartitionCount partitionCount ts
            tss.Length |> shouldEqual partitionCount
            Array.concat tss |> shouldEqual ts)

    [<Test>]
    let ``Array.splitWeighted`` () =
        Check.QuickThrowOnFail<uint16 [] * uint16>(fun (weights : uint16 [], arraySize : uint16) ->
            if weights = null || weights.Length = 0 then () else // expected failure case
            let weights = weights |> Array.map (fun w -> 1 + int w) // need weights > 0
            let arraySize = int arraySize
            let ts = [|1 .. arraySize|]
            let tss = Array.splitWeighted weights ts
            Array.concat tss |> shouldEqual ts)