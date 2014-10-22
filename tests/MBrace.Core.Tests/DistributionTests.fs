namespace Nessos.MBrace.Tests

open System
open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.Thespian

open Nessos.MBrace
open Nessos.MBrace.SampleRuntime

[<TestFixture>]
module ``Distribution Tests`` =

    type Canceller = Actors.EventActor<unit>

    let mutable runtime : MBraceRuntime option = None

    [<TestFixtureSetUp>]
    let init () =
        MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + @"\..\..\bin\MBrace.SampleRuntime.exe"
        runtime <- Some <| MBraceRuntime.InitLocal(4)

    [<TestFixtureTearDown>]
    let fini () =
        runtime |> Option.iter (fun r -> r.Kill())
        runtime <- None

    let run (workflow : Cloud<'T>) = Option.get(runtime).RunAsync workflow |> Async.Catch |> Async.RunSynchronously
    let runCts (workflow : Canceller -> Cloud<'T>) =
        let cts = new CancellationTokenSource()
        let observable, canceller = Canceller.Init()
        let _ = observable.Subscribe(fun () -> cts.Cancel())
        Option.get(runtime).RunAsync(workflow canceller, cancellationToken = cts.Token) |> Async.Catch |> Async.RunSynchronously

    [<Test>]
    let ``Parallel : empty input`` () =
        run (Cloud.Parallel [||]) |> Choice.shouldEqual [||]

//    [<Test>]
//    let ``Parallel : simple inputs`` () =
//        cloud {
//            do! Cloud.Sleep 20000
//            let f i = cloud { return i + 1 }
//            let! results = Array.init 100 f |> Cloud.Parallel
//            return Array.sum results
//        } |> run |> Choice.shouldEqual 5050
//
//    [<Test>]
//    let ``Parallel : simple nested`` () =
//        cloud {
//            let f i j = cloud { return i + j + 2 }
//            let cluster i = Array.init 10 (f i) |> Cloud.Parallel
//            let! results = Array.init 10 cluster |> Cloud.Parallel
//            return Array.concat results |> Array.sum
//        } |> run |> Choice.shouldEqual 1100
//
//    [<Test>]
//    let ``Parallel : simple exception`` () =
//        cloud {
//            let f i = cloud { return if i = 55 then invalidOp "failure" else i + 1 }
//            let! results = Array.init 100 f |> Cloud.Parallel
//            return Array.sum results
//        } |> run |> Choice.shouldFailwith<_, InvalidOperationException>