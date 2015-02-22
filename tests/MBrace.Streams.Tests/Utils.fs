namespace MBrace.Streams.Tests

open NUnit.Framework

[<AutoOpen>]
module Utils =

    let shouldfail (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "should fail but was '%A'" v)
        with _ -> ()

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "should fail but was '%A'" v)
        with :? 'Exn -> ()

    /// type safe equality tester
    let shouldEqual (expected : 'T) (input : 'T) = 
        if expected = input then ()
        else
            raise <| new AssertionException(sprintf "expected '%A' but was '%A'." expected input)

    let shouldBe (pred : 'T -> bool) (input : 'T) =
        if pred input then ()
        else
            raise <| new AssertionException(sprintf "value '%A' does not match predicate." input)

    [<RequireQualifiedAccess>]
    module Choice =

        let protect (f : unit -> 'T) =
            try f () |> Choice1Of2 with e -> Choice2Of2 e

        let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 v' -> shouldEqual value v'
            | Choice2Of2 e -> raise e

        let shouldBe (pred : 'T -> bool) (input : Choice<'T, exn>) =
            match input with
            | Choice1Of2 t when pred t -> ()
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
            | Choice2Of2 e -> raise e

        let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "Expected exception, but was value '%A'." t)
            | Choice2Of2 (:? 'Exn) -> ()
            | Choice2Of2 e -> raise e