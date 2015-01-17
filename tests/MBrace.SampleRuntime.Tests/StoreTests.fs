namespace MBrace.SampleRuntime.Tests

open System.Threading

open MBrace
open MBrace.Store.Tests
open MBrace.SampleRuntime

open NUnit.Framework

[<TestFixture>]
type ``Store tests`` () =
    inherit ``MBrace store tests``()

    let mutable runtime : MBraceRuntime option = None

    [<TestFixtureSetUp>]
    member __.Init () =
        MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"
        runtime <- Some <| MBraceRuntime.InitLocal(4)
        do Thread.Sleep 2000

    [<TestFixtureTearDown>]
    member __.Fini () =
        runtime |> Option.iter (fun r -> r.KillAllWorkers())
        runtime <- None

    override __.Run(wf : Cloud<'T>, ?ct : CancellationToken) =
        match runtime with
        | None -> invalidOp "no runtime state available."
        | Some r -> r.Run(wf, ?cancellationToken = ct)

    override __.RunLocal(wf : Cloud<'T>) =
        match runtime with
        | None -> invalidOp "no runtime state available."
        | Some r -> r.RunLocal(wf)