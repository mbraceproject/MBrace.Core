namespace MBrace.Core.Tests

open System
open System.Threading

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "444"

[<TestFixture>]
module ``Misc MBrace Core Tests`` =

    let _ = ThreadPool.SetMinThreads(100,100)

    let run (wf : Cloud<'T>) = Cloud.RunSynchronously(wf, ResourceRegistry.Empty, new InMemoryCancellationToken())

    [<Test; Repeat(5)>]
    let ``DomainLocal factories should be atomic`` () =
        let c = ref 0
        let dl = DomainLocal.Create(fun () -> incr c ; !c)
        [|1 .. 100|] |> Array.Parallel.map (fun _ -> Thread.Sleep 10 ; dl.Value) |> ignore
        test <@ !c = 1 @>
        test <@ dl.Value = 1 @>

    [<Test; Repeat(5)>]
    let ``DomainLocal factories should be atomic (Cloud)`` () =
        let c = ref 0
        let dl = DomainLocal.Create(local { return (incr c ; !c) })
        [|1 .. 20|] |> Array.Parallel.map (fun _ -> run dl.Value) |> ignore
        test <@ !c = 1 @>
        test <@ run dl.Value = 1 @>