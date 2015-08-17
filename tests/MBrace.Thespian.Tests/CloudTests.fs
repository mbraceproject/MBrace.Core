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

#nowarn "444"

type ``MBrace Thespian Cloud Tests`` () as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 3000)

    let session = new RuntimeSession(workerCount = 4)

    let runOnCloud (wf : Cloud<'T>) = self.RunOnCloud wf
    let repeat f = repeat self.Repeats f

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsTargetWorkerSupported = true

    override __.RunOnCloud (workflow : Cloud<'T>) = 
        session.Runtime.RunOnCloudAsync (workflow)
        |> Async.Catch
        |> Async.RunSync

    override __.RunOnCloud (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            try return! runtime.RunOnCloudAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunOnCloudWithLogs(workflow : Cloud<unit>) =
        let task = session.Runtime.CreateCloudTask(workflow)
        do task.Result
        task.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess(workflow)

    override __.FsCheckMaxTests = 10
    override __.UsesSerialization = true
    override __.IsSiftedWorkflowSupported = true
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif


[<TestFixture>]
type ``MBrace Thespian Specialized Cloud Tests`` () =

    let session = new RuntimeSession(workerCount = 4)

    let repeat f = repeat 10 f

    let runOnCloud (wf : Cloud<'T>) = session.Runtime.RunOnCloud wf

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    [<Test>]
    member __.``1. Runtime : Get worker count`` () =
        runOnCloud (Cloud.GetWorkerCount()) |> shouldEqual (session.Runtime.Workers.Length)

    [<Test>]
    member __.``1. Runtime : Get current worker`` () =
        runOnCloud Cloud.CurrentWorker |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Get process id`` () =
        runOnCloud (Cloud.GetProcessId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Get task id`` () =
        runOnCloud (Cloud.GetJobId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Task Log Observable`` () =
        let workflow = cloud {
            let job i = local {
                for j in 1 .. 100 do
                    do! Cloud.Logf "Job %d, iteration %d" i j
            }

            do! Cloud.Sleep 5000
            do! Cloud.Parallel [for i in 1 .. 20 -> job i] |> Cloud.Ignore
            do! Cloud.Sleep 2000
        }

        let ra = new ResizeArray<CloudLogEntry>()
        let task = session.Runtime.CreateCloudTask(workflow)
        use d = task.Logs.Subscribe(fun e -> ra.Add(e))
        do task.Result
        ra |> Seq.filter (fun e -> e.Message.Contains "Job") |> Seq.length |> shouldEqual 2000

    [<Test>]
    member __.``1. Runtime : Additional Resources`` () =
        let workflow = cloud { return! Cloud.GetResource<int> () }
        session.Runtime.RunOnCloud(workflow, additionalResources = resource { yield 42 }) |> shouldEqual 42

    [<Test>]
    member __.``2. Fault Tolerance : map/reduce`` () =
        repeat (fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateCloudTask(WordCount.run 20 WordCount.mapReduceRec)
            do Thread.Sleep 4000
            session.Chaos()
            t.Result |> shouldEqual 100)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 1`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateCloudTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
            do Thread.Sleep 5000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``3. Fault Tolerance : Custom fault policy 2`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let t = runtime.CreateCloudTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))
            do Thread.Sleep 5000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : targeted workers`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted jobs should fail regardless of fault policy
                return! Cloud.StartAsTask(Cloud.Sleep 20000, target = current, faultPolicy = FaultPolicy.InfiniteRetry())
            }

            do Thread.Sleep 1000
            let t = runtime.RunOnCloud (wf ())
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)