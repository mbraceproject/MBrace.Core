namespace MBrace.Thespian.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Thespian
open MBrace.CSharp.Tests

[<Category("AcceptanceTests")>]
type ``MBrace Thespian Cloud CSharp Tests`` () =
    inherit CloudTests()

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop()
    
    override __.Run(expr : Cloud<'T>) : 'T = session.Cluster.Run(expr, faultPolicy = FaultPolicy.NoRetry)
    override __.RunLocally(expr : Cloud<'T>) : 'T = session.Cluster.RunLocally(expr)
    override __.RunWithLogs(workflow : Cloud<unit>) =
        let job = session.Cluster.CreateProcess(workflow)
        do job.Result
        System.Threading.Thread.Sleep 1000
        job.GetLogs () |> Array.map CloudLogEntry.Format

[<Category("AcceptanceTests")>]
type ``MBrace Thespian CloudFlow CSharp Tests`` () =
    inherit CloudFlowTests()

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop()
      
    override __.FsCheckMaxNumberOfTests = 10  
    override __.FsCheckMaxNumberOfIOBoundTests = 10
    
    override __.Run(expr : Cloud<'T>) : 'T = session.Cluster.Run(expr, faultPolicy = FaultPolicy.NoRetry)
    override __.RunLocally(expr : Cloud<'T>) : 'T = session.Cluster.RunLocally(expr)
    override __.RunWithLogs(workflow : Cloud<unit>) =
        let job = session.Cluster.CreateProcess(workflow)
        do job.Result
        System.Threading.Thread.Sleep 1000
        job.GetLogs () |> Array.map CloudLogEntry.Format