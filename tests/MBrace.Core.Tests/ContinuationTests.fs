namespace Nessos.MBrace.Tests

open System
open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.MBrace

#nowarn "444"

[<TestFixture>]
[<Category("ContinuationTests")>]
module ``Continuation Tests`` =

    //
    //  Simple expression execution
    //

    [<Test>]
    let ``return value`` () =
        cloud { return 1 + 1 } |> Cloud.RunProtected |> Choice.shouldEqual 2

    [<Test>]
    let ``return exception`` () =
        cloud { return 1  / 0 } |> Cloud.RunProtected |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``side effect`` () =
        let cell = ref 0
        let comp = cloud { incr cell } 
        !cell |> should equal 0
        comp |> Cloud.RunProtected |> Choice.shouldEqual ()
        !cell |> should equal 1

    [<Test>]
    let ``uncaught exception`` () =
        cloud { ignore (1 / 0) } |> Cloud.RunProtected |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``let binding`` () =
        cloud { let x = 1 + 1 in return x + x } |> Cloud.RunProtected |> Choice.shouldEqual 4

    [<Test>]
    let ``monadic binding`` () =
        cloud {
            let! x = cloud { return 1 + 1 }
            return x + x
        } |> Cloud.RunProtected |> Choice.shouldEqual 4

    [<Test>]
    let ``exception in nested binding`` () =
        cloud {
            let! x = cloud { return 1 / 0 }
            return x + x
        }  |> Cloud.RunProtected |> Choice.shouldFailwith<_, DivideByZeroException>

    [<Test>]
    let ``monadic binding with exception on continuation`` () =
        cloud {
            let! x = cloud { return 1 + 1 }
            do invalidOp "failure"
            return x + x
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidOperationException>


    [<Test>]
    let ``combined workflows`` () =
        cloud {
            let cell = ref 0
            do! cloud { incr cell }
            do! cloud { incr cell }
            return !cell
        } |> Cloud.RunProtected |> Choice.shouldEqual 2

    [<Test>]
    let ``exception in nested binding 2`` () =
        cloud {
            let! x = Cloud.Raise (new IndexOutOfRangeException())
            return x + 1
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, IndexOutOfRangeException>

    [<Test>]
    let ``cancellation`` () =
        let cell = ref 0
        let result =
            Cloud.RunProtected(fun cts ->
                cloud {
                    do cts.Cancel()
                    do! cloud { incr cell }
                })

        !cell |> should equal 0
        result |> Choice.shouldFailwith<_, OperationCanceledException>

    [<Test>]
    let ``try with handled exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return None
            with :? IndexOutOfRangeException as e -> return (Some e)
        } |> Cloud.RunProtected |> Choice.shouldMatch Option.isSome

    [<Test>]
    let ``try with unhandled exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return false
            with :? DivideByZeroException -> return true
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, IndexOutOfRangeException>

    [<Test>]
    let ``try with handler raising new exception`` () =
        cloud {
            try
                do raise <| new IndexOutOfRangeException()
                return false
            with :? IndexOutOfRangeException -> return! Cloud.Raise(new DivideByZeroException())
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, DivideByZeroException>

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
        } |> Cloud.RunProtected |> Choice.shouldEqual true

        !cell |> should equal 1

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
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, IndexOutOfRangeException>

        !cell |> should equal 1

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
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidCastException>


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
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidCastException>

    [<Test>]
    let ``for loop`` () =
        let cell = ref 0
        let comp = cloud {
            for i in 1 .. 100 do
                do! cloud { cell := !cell + i }
        }
        !cell |> should equal 0
        Cloud.RunProtected comp |> Choice.shouldEqual ()
        !cell |> should equal 5050

    [<Test>]
    let ``for loop on empty inputs`` () =
        let cell = ref 0
        let comp = cloud {
            for i in (incr cell ; []) do
                do! cloud { cell := !cell + i }
        }
        !cell |> should equal 0
        Cloud.RunProtected comp |> Choice.shouldEqual ()
        !cell |> should equal 1

    [<Test>]
    let ``for loop on null inputs`` () =
        cloud {
            for i in Unchecked.defaultof<int list> do
                do! cloud { return () }
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, ArgumentNullException>

    [<Test>]
    let ``for loop with exception`` () =
        let cell = ref 0
        cloud {
            for i in 1 .. 100 do
                incr cell
                if i = 55 then return invalidOp "failure"
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidOperationException>

        !cell |> should equal 55

    [<Test>]
    let ``for loop with cancellation`` () =
        let cell = ref 0
        Cloud.RunProtected(fun cts ->
            cloud {
                for i in 1 .. 100 do
                    incr cell
                    if i = 55 then cts.Cancel()
                
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !cell |> should equal 55

    [<Test>]
    let ``while loop`` () =
        cloud {
            let cell = ref 0
            while !cell < 100 do
                incr cell

            return !cell
        } |> Cloud.RunProtected |> Choice.shouldEqual 100

    [<Test>]
    let ``while loop with exception on predicate`` () =
        let cell = ref 0
        cloud {
            while (if !cell < 55 then true else invalidOp "failure") do
                incr cell
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidOperationException>
        !cell |> should equal 55

    [<Test>]
    let ``while loop with exception on body`` () =
        let cell = ref 0
        cloud {
            while true do
                incr cell
                if !cell = 55 then invalidOp "failure"
        } |> Cloud.RunProtected |> Choice.shouldFailwith<_, InvalidOperationException>
        !cell |> should equal 55

    [<Test>]
    let ``while loop with cancellation`` () =
        let cell = ref 0
        Cloud.RunProtected(fun cts ->
            cloud {
                while true do
                    incr cell
                    if !cell = 55 then cts.Cancel()
                
            }) |> Choice.shouldFailwith<_, OperationCanceledException>

        !cell |> should equal 55

    [<Test>]
    let ``use binding`` () =
        cloud {
            let! r1, d = cloud {
                use d = new DummyDisposable()
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> Cloud.RunProtected |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use! binding`` () =
        cloud {
            let! r1, d = cloud {
                use! d = cloud { return new DummyDisposable() }
                return d.IsDisposed, d 
            }

            return r1, d.IsDisposed
        } |> Cloud.RunProtected |> Choice.shouldEqual (false, true)

    [<Test>]
    let ``use binding with exception`` () =
        cloud {
            let d = new DummyDisposable ()
            try
                use d = d
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> Cloud.RunProtected |> Choice.shouldEqual true

    [<Test>]
    let ``use! binding with exception`` () =
        cloud {
            let d = new DummyDisposable ()
            try
                use! d = cloud { return d }
                do failwith ""
                return d.IsDisposed

            with _ -> return d.IsDisposed

        } |> Cloud.RunProtected |> Choice.shouldEqual true


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
            Cloud.RunProtected (factC i) |> Choice.shouldEqual (fact i)

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
            Cloud.RunProtected (fibC i) |> Choice.shouldEqual (fib i)

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
            Cloud.RunProtected(ackermannC i i) |> Choice.shouldEqual (ackermann i i)


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

        Cloud.RunProtected (int2Peano -1) |> Choice.shouldFailwith<_, ArgumentException>

        for i = 0 to 10 do
            Cloud.RunProtected (cloud { let! p = int2Peano i in return p.Value }) |> Choice.shouldEqual i


    [<Test>]
    let ``stack overflow`` () =
        let rec diveTo n = cloud {
            if n = 0 then return 0
            else
                let! r = diveTo (n-1)
                return 1 + r
        }

        Cloud.RunProtected(diveTo 100000) |> Choice.shouldEqual 100000

    [<Test>]
    let ``deep exception`` () =
        let rec diveRaise n = cloud {
            if n = 0 then return invalidOp "failure"
            else
                let! r = diveRaise (n-1)
                return 1 + r
        }

        Cloud.RunProtected(diveRaise 100000) |> Choice.shouldFailwith<_, InvalidOperationException>

    [<Test>]
    let ``deep cancellation`` () =
        let rec diveRaise n (cts : CancellationTokenSource) = cloud {
            if n = 0 then cts.Cancel() ; return 0
            else
                let! r = diveRaise (n-1) cts
                return 1 + r
        }

        Cloud.RunProtected(diveRaise 100000) |> Choice.shouldFailwith<_, OperationCanceledException>


    [<Test>]
    let ``runtime resources`` () =
        Cloud.RunProtected(Cloud.GetWorkerCount()) |> Choice.shouldFailwith<_, Runtime.ResourceNotFoundException>

//    [<Test>]
//    let ``storage resouces`` () =
//        Cloud.RunProtected(CloudRef.New 0) |> Choice.shouldFailwith<_, Runtime.ResourceNotFoundException>
