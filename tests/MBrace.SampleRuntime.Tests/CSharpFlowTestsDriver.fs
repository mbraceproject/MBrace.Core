namespace MBrace.SampleRuntime.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Streams.CSharp.Tests

type ``SampleRuntime CloudStream CSharp Tests`` () =
    inherit CloudStreamsTests()

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.Run(workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocal(workflow : Cloud<'T>) = session.Runtime.RunLocal(workflow)
    override __.MaxNumberOfTests = 10