namespace MBrace.Thespian.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Core.Tests
open MBrace.Thespian

type ``MBrace Thespian Parallelism Tests`` () as self =
    inherit ``Distribution Tests`` (parallelismFactor = 20, delayFactor = 3000)

    let session = new RuntimeSession(nodes = 4)

    let runRemote (wf : Cloud<'T>) = self.RunRemote wf
    let repeat f = repeat self.Repeats f

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsTargetWorkerSupported = true

    override __.RunRemote (workflow : Cloud<'T>) = 
        session.Runtime.RunAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.RunRemote (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            return! runtime.RunAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
        } |> Async.RunSync

    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally(workflow)

    override __.Logs = session.Logger :> _
    override __.FsCheckMaxTests = 10
    override __.UsesSerialization = true
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

    [<Test>]
    member __.``Z4. Runtime : Get worker count`` () =
        runRemote (Cloud.GetWorkerCount()) |> Choice.shouldEqual (session.Runtime.Workers.Length)

    [<Test>]
    member __.``Z4. Runtime : Get current worker`` () =
        runRemote Cloud.CurrentWorker |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get process id`` () =
        runRemote (Cloud.GetProcessId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z4. Runtime : Get task id`` () =
        runRemote (Cloud.GetJobId()) |> Choice.shouldBe (fun _ -> true)

    [<Test>]
    member __.``Z5. Fault Tolerance : map/reduce`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateProcess(WordCount.run 20 WordCount.mapReduceRec)
            do Thread.Sleep 4000
            session.Chaos()
            t.Result |> shouldEqual 100)

    [<Test>]
    member __.``Z5. Fault Tolerance : Custom fault policy 1`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateProcess(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
            do Thread.Sleep 5000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``Z5. Fault Tolerance : Custom fault policy 2`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateProcess(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
            do Thread.Sleep 5000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``Z5. Fault Tolerance : targeted workers`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted jobs should fail regardless of fault policy
                return! Cloud.StartAsTask(Cloud.Sleep 20000, target = current, faultPolicy = FaultPolicy.InfiniteRetry())
            }

            do Thread.Sleep 1000
            let t = runtime.Run (wf ())
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)