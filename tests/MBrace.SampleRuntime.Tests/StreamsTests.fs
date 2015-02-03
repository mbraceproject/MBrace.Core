namespace MBrace.SampleRuntime.Tests

open System.IO

open NUnit.Framework

open MBrace
open MBrace.Streams.Tests
open MBrace.SampleRuntime

[<Category("CloudStreams.Cluster")>]
type ``SampleRuntime Streams Tests`` () =
    inherit ``CloudStreams tests`` ()

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()
      
    override __.FsCheckMaxNumberOfTests = 10  
    override __.Run(expr : Cloud<'T>) : 'T = session.Runtime.Run(expr, faultPolicy = FaultPolicy.NoRetry)
    override __.RunLocal(expr : Cloud<'T>) : 'T = session.Runtime.RunLocal(expr)