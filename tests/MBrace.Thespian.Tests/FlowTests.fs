namespace MBrace.Thespian.Tests

open System.IO

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Thespian

[<Category("ThespianClusterTests")>]
type ``MBrace Thespian Flow Tests`` () =
    inherit ``CloudFlow tests`` ()

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()
      
    override __.FsCheckMaxNumberOfTests = 10  
    override __.FsCheckMaxNumberOfIOBoundTests = 10
    override __.IsSupportedStorageLevel level = session.Cluster.GetResource<ICloudValueProvider>().IsSupportedStorageLevel level
    override __.Run(expr : Cloud<'T>) : 'T = session.Cluster.Run(expr, faultPolicy = FaultPolicy.NoRetry)
    override __.RunLocally(expr : Cloud<'T>) : 'T = session.Cluster.RunLocally(expr)
    override __.RunWithLogs(workflow : Cloud<unit>) =
        let job = session.Cluster.CreateProcess(workflow)
        do job.Result
        System.Threading.Thread.Sleep 1000
        job.GetLogs () |> Array.map CloudLogEntry.Format