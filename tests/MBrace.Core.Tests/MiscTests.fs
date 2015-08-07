namespace MBrace.Core.Tests

open System
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "444"

[<TestFixture>]
module ``Misc MBrace Core Tests`` =

    let run (wf : Cloud<'T>) = Cloud.RunSynchronously(wf, ResourceRegistry.Empty, new InMemoryCancellationToken())

    [<Test; Repeat(5)>]
    let ``DomainLocal factories should be atomic`` () =
        let c = ref 0
        let dl = DomainLocal.Create(fun () -> incr c ; !c)
        [|1 .. 100|] |> Array.Parallel.map (fun _ -> Thread.Sleep 10 ; dl.Value) |> ignore
        !c |> shouldEqual 1
        dl.Value |> shouldEqual 1