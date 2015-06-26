namespace MBrace.Thespian.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.CSharp.Tests

type ``MBrace Thespian CSharp Tests`` () =
    inherit ``SimpleTests`` ()

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.Run(workflow : Cloud<'T>) = session.Runtime.Run workflow