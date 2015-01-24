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

    let session = new RuntimeSession(nodes = 4)

    let run (wf : Cloud<'T>) = self.Run wf

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsTargetWorkerSupported = true

    override __.Run (workflow : Cloud<'T>) = 
        session.Runtime.RunAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.Run (workflow : ICancellationTokenSource -> Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let dcts = DistributedCancellationTokenSource.Init()
            let icts = { new ICancellationTokenSource with member __.Cancel() = dcts.Cancel () }
            let ct = dcts.GetLocalCancellationToken()
            return! runtime.RunAsync(workflow icts, cancellationToken = ct) |> Async.Catch
        } |> Async.RunSync

    override __.RunLocal(workflow : Cloud<'T>) = session.Runtime.RunLocal(workflow)

    override __.Logs = session.Logger :> _

    [<Test>]
    member __.``Z4. Runtime : Get worker count`` () =
        run (Cloud.GetWorkerCount()) |> Choice.shouldEqual (session.Runtime.Workers.Length)

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
        let runtime = session.Runtime
        let t = runtime.RunAsTask(WordCount.run 20 WordCount.mapReduceRec)
        do Thread.Sleep 4000
        runtime.KillAllWorkers()
        runtime.AppendWorkers 4
        t.Result |> shouldEqual 100

    [<Test>]
    [<Repeat(Config.repeats)>]
    member __.``Z5. Fault Tolerance : Custom fault policy 1`` () =
        let runtime = session.Runtime
        let t = runtime.RunAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
        do Thread.Sleep 4000
        runtime.KillAllWorkers()
        runtime.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>

    [<Test>]
    [<Repeat(Config.repeats)>]
    member __.``Z5. Fault Tolerance : Custom fault policy 2`` () =
        let runtime = session.Runtime
        let t = runtime.RunAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
        do Thread.Sleep 4000
        runtime.KillAllWorkers()
        runtime.AppendWorkers 4
        Choice.protect (fun () -> t.CorrectResult) |> Choice.shouldFailwith<_, FaultException>