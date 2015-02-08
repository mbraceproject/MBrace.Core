namespace MBrace.SampleRuntime.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework

open MBrace
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Runtime
open MBrace.Tests
open MBrace.SampleRuntime
open MBrace.SampleRuntime.Actors

type ``SampleRuntime Parallelism Tests`` () as self =
    inherit ``Parallelism Tests`` (nParallel = 20)

    let session = new RuntimeSession(nodes = 4)

    let run (wf : Cloud<'T>) = self.Run wf
    let repeat f = repeat self.Repeats f

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsTargetWorkerSupported = true

    override __.Run (workflow : Cloud<'T>) = 
        session.Runtime.RunAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            return! runtime.RunAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
        } |> Async.RunSync

    override __.RunLocal(workflow : Cloud<'T>) = session.Runtime.RunLocal(workflow)

    override __.Logs = session.Logger :> _
    override __.FsCheckMaxTests = 10
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

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
        run (Cloud.GetJobId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z5. Fault Tolerance : map/reduce`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.StartAsTask(WordCount.run 20 WordCount.mapReduceRec)
            do Thread.Sleep 4000
            runtime.KillAllWorkers()
            runtime.AppendWorkers 4
            t.Result |> shouldEqual 100)

    [<Test>]
    member __.``Z5. Fault Tolerance : Custom fault policy 1`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.StartAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
            do Thread.Sleep 4000
            runtime.KillAllWorkers()
            runtime.AppendWorkers 4
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``Z5. Fault Tolerance : Custom fault policy 2`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.StartAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
            do Thread.Sleep 4000
            runtime.KillAllWorkers()
            runtime.AppendWorkers 4
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)