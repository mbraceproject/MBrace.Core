namespace MBrace.Thespian.Tests

open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Thespian

open NUnit.Framework

type ``MBrace Thespian FileStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.FileStore = session.Runtime.GetResource<ICloudFileStore>()
    override __.Serializer = session.Runtime.GetResource<ISerializer>()

    override __.RunOnCloud (workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow


type ``MBrace Thespian CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.RunOnCloud (workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow
    override __.IsSupportedLevel _ = true


type ``MBrace Thespian Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.RunOnCloud (workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow

#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

type ``MBrace Thespian Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.RunOnCloud (workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow

type ``MBrace Thespian Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsInMemoryFixture = false
    override __.RunOnCloud (workflow : Cloud<'T>) = session.Runtime.RunOnCloud workflow
    override __.RunOnCurrentProcess(workflow : Cloud<'T>) = session.Runtime.RunOnCurrentProcess workflow
