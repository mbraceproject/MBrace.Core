namespace MBrace.Thespian.Tests

open System
open System.IO
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Tests
open MBrace.Thespian

[<TestFixture>]
module ``MBrace Thespian Misc Tests`` =

    [<TestFixtureSetUp>]
    let init () =
        do RuntimeSession.Init()

    [<Test>]
    let ``Management : spawn and connect to worker node`` () =
        let worker = MBraceWorker.Spawn(hostname = "127.0.0.1", port = 36767)
        try
            let worker' = MBraceWorker.Connect "mbrace://127.0.0.1:36767"
            worker'.IsIdle |> shouldEqual true
            worker' |> shouldEqual worker
        finally
            worker.Kill()

    [<Test>]
    let ``Management : connect to invalid URI.`` () =
        fun () -> 
            MBraceWorker.Connect "mbrace://127.0.0.1:80"
        |> shouldFailwith<_, Exception>

        fun () ->
            MBraceWorker.Connect "http://127.0.0.1:80"
        |> shouldFailwith<_, Exception>

        fun () ->
            MBraceWorker.Connect "garbage123"
        |> shouldFailwith<_, Exception>

    [<Test>]
    let ``Management : run cluster hosted on worker node`` () =
        let master = MBraceWorker.Spawn()
        let worker1 = MBraceWorker.Spawn()
        let worker2 = MBraceWorker.Spawn()

        try
            let cluster = MBraceCluster.InitOnWorker(master)

            cluster.AttachWorker worker1
            cluster.AttachWorker worker2

            cluster.RunOnCloud(Cloud.Parallel [for i in 1 .. 10 -> cloud { return i }])
            |> shouldEqual [|1 .. 10|]

            cluster.Workers.Length |> shouldEqual 2

            let cluster' = MBraceCluster.Connect(worker1.Uri)
            cluster'.Workers.Length |> shouldEqual 2

        finally
            master.Kill() ; worker1.Kill() ; worker2.Kill()

    [<Test>]
    let ``Management : Attach new nodes to a cluster`` () =
        let cluster = MBraceCluster.InitOnWorker()
        try
            cluster.AttachNewLocalWorkers(3)
            cluster.Workers |> Array.map (fun w -> w.WorkerManager) |> Array.length |> shouldEqual 3
        finally
           cluster.KillAllWorkers()