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

    override __.FileStore = session.Cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.Cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = platformId = System.PlatformID.Unix

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow


type ``MBrace Thespian CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
    override __.IsSupportedLevel _ = true


type ``MBrace Thespian Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsSupportedNamedLookup = false
    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow

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

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
    override __.IsSupportedNamedLookup = false

type ``MBrace Thespian Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsInMemoryFixture = false
    override __.IsSupportedNamedLookup = false
    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
