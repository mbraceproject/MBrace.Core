namespace MBrace.Flow.Tests

open NUnit.Framework
open FsCheck

[<System.Runtime.CompilerServices.InternalsVisibleTo("MBrace.Flow.CSharp.Tests")>]
do()

[<AutoOpen>]
module Utils =

    let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
    let isAppVeyorInstance = System.Environment.GetEnvironmentVariable("APPVEYOR") = "TRUE"
    let isTravisInstance = System.Environment.GetEnvironmentVariable("TRAVIS") = "true"

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

type internal Check =
    static member QuickThrowOnFailureConfig(maxNumber) = { Config.QuickThrowOnFailure with MaxTest = maxNumber }

    /// quick check methods with explicit type annotation
    static member QuickThrowOnFail<'T> (f : 'T -> unit, ?maxNumber) = 
        match maxNumber with
        | None -> Check.QuickThrowOnFailure f
        | Some mxrs -> Check.One({ Config.QuickThrowOnFailure with MaxTest = mxrs }, f)

    /// quick check methods with explicit type annotation
    static member QuickThrowOnFail<'T> (f : 'T -> bool, ?maxNumber) = 
        match maxNumber with
        | None -> Check.QuickThrowOnFailure f
        | Some mxrs -> Check.One({ Config.QuickThrowOnFailure with MaxTest = mxrs }, f)