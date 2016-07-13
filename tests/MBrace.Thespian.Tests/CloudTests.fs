namespace MBrace.Thespian.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Core.Internals
open MBrace.Flow
open MBrace.Library.Protected
open MBrace.Runtime
open MBrace.Core.Tests
open MBrace.Thespian

#nowarn "444"

[<Category("ThespianClusterTests")>]
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
        session.Cluster.RunAsync (workflow)
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let runtime = session.Cluster
            let cts = runtime.CreateCancellationTokenSource()
            try return! runtime.RunAsync(workflow cts, cancellationToken = cts.Token)
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let job = session.Cluster.CreateProcess(workflow)
        do job.Result
        job.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally(workflow)

    override __.FsCheckMaxTests = 10
    override __.UsesSerialization = true
    override __.IsSiftedWorkflowSupported = true
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif


[<TestFixture; Category("ThespianClusterTests")>]
type ``MBrace Thespian Specialized Cloud Tests`` () =

    let session = new RuntimeSession(workerCount = 4)

    let repeat f = repeat 10 f

    let runOnCloud (wf : Cloud<'T>) = session.Cluster.Run wf

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    [<Test>]
    member __.``1. Runtime : Get worker count`` () =
        test <@ runOnCloud (Cloud.GetWorkerCount()) = session.Cluster.Workers.Length @>

    [<Test>]
    member __.``1. Runtime : Get current worker`` () =
        runOnCloud Cloud.CurrentWorker |> ignore

    [<Test>]
    member __.``1. Runtime : Get process id`` () =
        runOnCloud (Cloud.GetCloudProcessId()) |> ignore

    [<Test>]
    member __.``1. Runtime : Get work item id`` () =
        runOnCloud (Cloud.GetWorkItemId()) |> ignore

    [<Test>]
    member __.``1. Runtime : Worker Log Observable`` () =
        let cluster = session.Cluster
        let worker = cluster.Workers.[0]
        let ra = new ResizeArray<SystemLogEntry>()
        use d = worker.SystemLogs.Subscribe ra.Add
        cluster.Run(cloud { return () }, target = worker)
        System.Threading.Thread.Sleep 2000
        test <@ ra.Count > 0 @>

    [<Test>]
    member __.``1. Runtime : Cluster Log Observable`` () =
        let cluster = session.Cluster
        let ra = new ResizeArray<SystemLogEntry>()
        use d = cluster.SystemLogs.Subscribe ra.Add
        cluster.Run(Cloud.ParallelEverywhere(cloud { return 42 }) |> Cloud.Ignore)
        System.Threading.Thread.Sleep 2000
        test <@ ra.Count >= cluster.Workers.Length @>

    [<Test>]
    member __.``1. Runtime : CloudProcess Log Observable`` () =
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
        let job = session.Cluster.CreateProcess(workflow)
        use d = job.Logs.Subscribe(fun e -> ra.Add(e))
        do job.Result
        let length = ra |> Seq.filter (fun e -> e.Message.Contains "Work item") |> Seq.length
        test <@ length = 2000 @>

    [<Test>]
    member __.``1. Runtime : Additional Resources`` () =
        let workflow = cloud { return! Cloud.GetResource<int> () }
        let result = session.Cluster.Run(workflow, additionalResources = resource { yield 42 }) 
        test <@ result = 42 @>

    [<Test>]
    member __.``2. Fault Tolerance : map/reduce`` () =
        repeat (fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let t = runtime.CreateProcess(cloud {
                do! f.ForceAsync true
                return! WordCount.run 20 WordCount.mapReduceRec
            }, faultPolicy = FaultPolicy.InfiniteRetries())
            while not f.Value do Thread.Sleep 1000
            do Thread.Sleep 1000
            session.Chaos()
            test <@ t.Result = 100 @>)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 1`` () =
        repeat(fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let t = runtime.CreateProcess(cloud {
                do! f.ForceAsync true
                do! Cloud.Sleep 20000
            }, faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            raises<FaultException> <@ t.Result @>)

    [<Test>]
    member __.``2. Fault Tolerance : Custom fault policy 2`` () =
        repeat(fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let t = runtime.CreateProcess(cloud {
                return! 
                    Cloud.WithFaultPolicy FaultPolicy.NoRetry
                        (cloud { 
                            do! f.ForceAsync true
                            return! Cloud.Sleep 20000 <||> Cloud.Sleep 20000
                        })
            })
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            raises<FaultException> <@ t.Result @>)

    [<Test>]
    member __.``2. Fault Tolerance : targeted workers`` () =
        repeat(fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let wf () = cloud {
                let! current = Cloud.CurrentWorker
                // targeted work items should fail regardless of fault policy
                return! 
                    Cloud.CreateProcess(cloud { 
                        do! f.ForceAsync true 
                        do! Cloud.Sleep 20000 }, target = current, faultPolicy = FaultPolicy.InfiniteRetries())
            }

            let t = runtime.Run (wf ())
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            raises<FaultException> <@ t.Result @>)

    [<Test>]
    member __.``2. Fault Tolerance : faulted process status`` () =
        repeat (fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let wf () = cloud {
                let worker () = cloud {
                    do! f.ForceAsync true 
                    do! Cloud.Sleep 60000
                }

                return! Cloud.ParallelEverywhere(worker())
            }

            let t = runtime.CreateProcess (wf (), faultPolicy = FaultPolicy.NoRetry)
            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            raises<FaultException> <@ t.Result @>
            test <@ t.Status = CloudProcessStatus.Faulted @>)

    [<Test>]
    member __.``2. Fault Tolerance : persistedCloudFlow`` () =
        let runtime = session.Cluster
        let n = 1000000
        let wf () = cloud {
            let data = [|1..n|]
            return!
                CloudFlow.OfArray data
                |> CloudFlow.persist StorageLevel.Disk
        }
        let flow = runtime.Run (wf ())
        session.Chaos()
        let result = 
            flow
            |> CloudFlow.length
            |> runtime.Run
        test <@ result = int64 n @>

    [<Test>]
    member __.``2. Fault Tolerance : fault data`` () =
        let faultData = session.Cluster.Run(Cloud.TryGetFaultData()) 
        test <@ Option.isNone faultData @>

        repeat(fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let t = 
                runtime.CreateProcess(
                    cloud {
                        do! f.ForceAsync true
                        do! Cloud.Sleep 5000
                        return! Cloud.TryGetFaultData()
                    }, faultPolicy = FaultPolicy.WithMaxRetries 1)

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            test <@ match t.Result with Some { NumberOfFaults = 1 } -> true | _ -> false @>)

    [<Test>]
    member __.``2. Fault Tolerance : protected parallel workflows`` () =
        repeat(fun () ->
            let runtime = session.Cluster
            let f = runtime.Store.CloudAtom.Create(false)
            let task i = cloud {
                do! f.ForceAsync true
                do! Cloud.Sleep 5000
                return i
            }

            let cloudProcess =
                [for i in 1 .. 10 -> task i]
                |> Cloud.ProtectedParallel
                |> runtime.CreateProcess

            while not f.Value do Thread.Sleep 1000
            session.Chaos()
            test 
                <@
                    cloudProcess.Result 
                    |> Array.forall (function FaultException _ -> true | _ -> false)
                @>)