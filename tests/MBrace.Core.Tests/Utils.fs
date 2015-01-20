namespace MBrace.Tests

open System.IO
open System.Threading

open NUnit.Framework
open FsUnit

open MBrace.Continuation
open MBrace.Store

module Config =

    [<Literal>]
#if DEBUG
    let repeats = 10
#else
    let repeats = 3
#endif

[<AutoOpen>]
module Utils =

    let shouldfail (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "should fail but was '%O'" v)
        with _ -> ()

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "should fail but was '%O'" v)
        with e -> should be instanceOfType<'Exn> e

    /// type safe equality tester
    let shouldEqual (expected : 'T) (input : 'T) = input |> should equal expected

    let shouldMatch (pred : 'T -> bool) (input : 'T) =
        if pred input then ()
        else
            raise <| new AssertionException(sprintf "value '%A' does not match predicate." input)

    type ISerializer with
        member s.Clone<'T>(t : 'T) =
            use m = new MemoryStream()
            s.Serialize(m, t, leaveOpen = true)
            m.Position <- 0L
            s.Deserialize<'T>(m, leaveOpen = true)
            

    [<RequireQualifiedAccess>]
    module Choice =

        let protect (f : unit -> 'T) =
            try f () |> Choice1Of2 with e -> Choice2Of2 e

        let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 v' -> should equal value v'
            | Choice2Of2 e -> should equal value e

        let shouldMatch (pred : 'T -> bool) (input : Choice<'T, exn>) =
            match input with
            | Choice1Of2 t when pred t -> ()
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
            | Choice2Of2 e -> should be instanceOfType<'T> e

        let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
            match input with
            | Choice1Of2 t -> raise <| new AssertionException(sprintf "Expected exception, but was value '%A'." t)
            | Choice2Of2 e -> should be instanceOfType<'Exn> e