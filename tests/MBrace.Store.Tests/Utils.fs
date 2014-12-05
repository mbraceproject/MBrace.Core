namespace Nessos.MBrace.Store.Tests

open NUnit.Framework

[<AutoOpen>]
module Utils =

    let shouldfail (f : unit -> 'T) =
        try let v = f () in raise <| new AssertionException(sprintf "should fail but was '%O'" v)
        with _ -> ()