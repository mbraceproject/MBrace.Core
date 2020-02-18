namespace MBrace.Thespian.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.Tests
open MBrace.Thespian

[<TestFixture; Category("AcceptanceTests")>]
module ``MBrace Thespian Misc Tests`` =

    [<OneTimeSetUp>]
    let init () =
        do RuntimeSession.Init()

    [<Test>]
    let ``Management : spawn and connect to worker node`` () =
        let worker = ThespianWorker.Spawn(hostname = "127.0.0.1", port = 36767)
        try
            let worker' = ThespianWorker.Connect "mbrace://127.0.0.1:36767"
            test <@ worker'.IsIdle = true @>
            test <@ worker' = worker @>
        finally
            worker.Kill()

    [<Test; Ignore("Test hanging")>]
    let ``Management : connect to invalid URI`` () =
        raises <@ ThespianWorker.Connect "mbrace://127.0.0.1:80" @>
        raises <@ ThespianWorker.Connect "http://127.0.0.1:80" @>
        raises <@ ThespianWorker.Connect "garbage123" @>

    [<Test>]
    let ``Management : run cluster hosted on worker node`` () =
        let master = ThespianWorker.Spawn()
        let worker1 = ThespianWorker.Spawn()
        let worker2 = ThespianWorker.Spawn()

        try
            let cluster = ThespianCluster.InitOnWorker(master)

            cluster.AttachWorker worker1
            cluster.AttachWorker worker2

            test <@ cluster.Run(Cloud.Parallel [for i in 1 .. 10 -> cloud { return i }]) = [|1 .. 10|] @>

            test <@ cluster.Workers.Length = 2 @>

            let cluster' = ThespianCluster.Connect(worker1.Uri)
            test <@ cluster'.Workers.Length = 2 @>

        finally
            master.Kill() ; worker1.Kill() ; worker2.Kill()

    [<Test>]
    let ``Management : Attach new nodes to a cluster`` () =
        let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 0, hostClusterStateOnCurrentProcess = false)
        try
            cluster.AttachNewLocalWorkers(3)
            test <@ cluster.Workers |> Array.map (fun w -> w.WorkerManager) |> Array.length = 3 @>
            test <@ cluster.MasterNode |> Option.isSome @>
        finally
           cluster.KillAllWorkers()
           cluster.MasterNode |> Option.iter (fun n -> n.Kill())