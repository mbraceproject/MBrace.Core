namespace MBrace.Thespian.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library.Protected
open MBrace.Runtime
open MBrace.Core.Tests
open MBrace.Thespian

#nowarn "444"

type ``MBrace Thespian Cloud Tests`` () as self =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 3000)

    let session = new RuntimeSession(workerCount = 4)

    let runOnCloud (wf : Cloud<'T>) = self.Run wf
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

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Runtime
            let cts = runtime.CreateCancellationTokenSource()
            try return! runtime.RunAsync(workflow cts, cancellationToken = cts.Token) |> Async.Catch
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let job = session.Runtime.Submit(workflow)
        do job.Result
        job.GetLogs () |> Array.map CloudLogEntry.Format

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

    let runOnCloud (wf : Cloud<'T>) = session.Runtime.Run wf

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
    member __.``1. Runtime : Get task id`` () =
        runOnCloud (Cloud.GetCloudProcessId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Get work item id`` () =
        runOnCloud (Cloud.GetWorkItemId()) |> shouldBe (fun _ -> true)

    [<Test>]
    member __.``1. Runtime : Worker Log Observable`` () =
        let cluster = session.Runtime
        let worker = cluster.Workers.[0]
        let ra = new ResizeArray<SystemLogEntry>()
        use d = worker.SystemLogs.Subscribe ra.Add
        cluster.Run(cloud { return () }, target = worker)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i > 0)

    [<Test>]
    member __.``1. Runtime : Cluster Log Observable`` () =
        let cluster = session.Runtime
        let ra = new ResizeArray<SystemLogEntry>()
        use d = cluster.SystemLogs.Subscribe ra.Add
        cluster.Run(Cloud.ParallelEverywhere(cloud { return 42 }) |> Cloud.Ignore)
        System.Threading.Thread.Sleep 2000
        ra.Count |> shouldBe (fun i -> i >= cluster.Workers.Length)

    [<Test>]
    member __.``1. Runtime : Task Log Observable`` () =
        let workflow = cloud {
            let workItem i = local {
                for j in 1 .. 100 do
                    do! Cloud.Logf "Work item %d, iteration %d" i j
            }

            do! Cloud.Sleep 5000
            do! Cloud.Parallel [for i in 1 .. 20 -> workItem i] |> Cloud.Ignore
            do! Cloud.Sleep 2000
        }

        let ra = new ResizeArray<CloudLogEntry>()
        let job = session.Runtime.Submit(workflow)
        use d = job.Logs.Subscribe(fun e -> ra.Add(e))
        do job.Result
        ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length |> shouldEqual 2000

    [<Test>]
    member __.``1. Runtime : Additional Resources`` () =
        let workflow = cloud { return! Cloud.GetResource<int> () }
        session.Runtime.Run(workflow, additionalResources = resource { yield 42 }) |> shouldEqual 42

    [<Test>]
    member __.``2. Fault Tolerance : map/reduce`` () =
        repeat (fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let t = runtime.Submit(cloud {
                do! f.Force true
                return! WordCount.run 20 WordCount.mapReduceRec
            }, faultPolicy = FaultPolicy.InfiniteRetries())
            while not f.Value do Thread.Sleep 1000
            do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldEqual 100)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 1`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let t = runtime.Submit(cloud {
                do! f.Force true
                do! Cloud.Sleep 20000
            }, faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``3. Fault Tolerance : Custom fault policy 2`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let t = runtime.Submit(cloud {
                return! 
                    Cloud.WithFaultPolicy FaultPolicy.NoRetry
                        (cloud { 
                            do! f.Force(true) 
                            return! Cloud.Sleep 20000 <||> Cloud.Sleep 20000
                        })
            })
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect (fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : targeted workers`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted work items should fail regardless of fault policy
                return! 
                    Cloud.StartAsCloudProcess(cloud { 
                        do! f.Force true 
                        do! Cloud.Sleep 20000 }, target = current, faultPolicy = FaultPolicy.InfiniteRetries())
            }

            let t = runtime.Run (wf ())
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            Choice.protect(fun () -> t.Result) |> Choice.shouldFailwith<_, FaultException>)

    [<Test>]
    member __.``2. Fault Tolerance : fault data`` () =
        session.Runtime.Run(Cloud.TryGetFaultData()) |> shouldBe Option.isNone

        repeat(fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let t = 
                runtime.Submit(
                    cloud {
                        do! f.Force true
                        do! Cloud.Sleep 5000
                        return! Cloud.TryGetFaultData()
                    })

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            t.Result |> shouldBe (function Some { NumberOfFaults = 1 } -> true | _ -> false))

    [<Test>]
    member __.``2. Fault Tolerance : protected parallel workflows`` () =
        repeat(fun () ->
            let runtime = session.Runtime
            let f = runtime.Store.Atom.Create(false)
            let task i = cloud {
                do! f.Force true
                do! Cloud.Sleep 5000
                return i
            }

            let t =
                [for i in 1 .. 10 -> task i]
                |> Cloud.ProtectedParallel
                |> runtime.CreateTask

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            t.Result 
            |> Array.forall (function FaultException _ -> true | _ -> false)
            |> shouldEqual true)
