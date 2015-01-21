namespace MBrace.SampleRuntime.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Tests
open MBrace.SampleRuntime
open MBrace.SampleRuntime.Actors

[<AutoOpen>]
module Utils =

    type System.Threading.Tasks.Task<'T> with
        member t.CorrectResult =
            try t.Result
            with :? AggregateException as e -> 
                raise e.InnerException

type ``SampleRuntime Parallelism Tests`` () as self =
    inherit ``Parallelism Tests`` (nParallel = 20)

    let mutable runtime : MBraceRuntime option = None

    let run (wf : Cloud<'T>) = self.Run wf

    [<TestFixtureSetUp>]
    member __.Init () =
        MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"
        runtime <- Some <| MBraceRuntime.InitLocal(4)

    [<TestFixtureTearDown>]
    member __.Fini () =
        runtime |> Option.iter (fun r -> r.KillAllWorkers())
        runtime <- None

    override __.IsTargetWorkerSupported = true

    override __.Run (workflow : Cloud<'T>) = 
        Option.get(runtime).RunAsync workflow 
        |> Async.Catch 
        |> Async.RunSynchronously

    override __.Run (workflow : ICancellationTokenSource -> Cloud<'T>) = 
        async {
            let runtime = Option.get runtime
            let dcts = DistributedCancellationTokenSource.Init()
            let icts = { new ICancellationTokenSource with member __.Cancel() = dcts.Cancel () }
            let ct = dcts.GetLocalCancellationToken()
            return! runtime.RunAsync(workflow icts, cancellationToken = ct) |> Async.Catch
        } |> Async.RunSync

    override __.RunLocal(workflow : Cloud<'T>) = Option.get(runtime).RunLocal(workflow)

    [<Test>]
    member __.``Z4. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (runtime.Value.Workers.Length)

    [<Test>]
    member __.``Z4. Runtime : Get current worker`` () =
        run Cloud.CurrentWorker |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get process id`` () =
        run (Cloud.GetProcessId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get task id`` () =
        run (Cloud.GetTaskId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    [<Repeat(Config.repeats)>]
    member __.``Z5. Fault Tolerance : map/reduce`` () =
        let t = runtime.Value.RunAsTask(WordCount.run 20 WordCount.mapReduceRec)
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        t.Result |> shouldEqual 100

    [<Test>]
    [<Repeat(Config.repeats)>]
    member __.``Z5. Fault Tolerance : Custom fault policy 1`` () =
        let t = runtime.Value.RunAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>

    [<Test>]
    [<Repeat(Config.repeats)>]
    member __.``Z5. Fault Tolerance : Custom fault policy 2`` () =
        let t = runtime.Value.RunAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
        do Thread.Sleep 4000
        runtime.Value.KillAllWorkers()
        runtime.Value.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>