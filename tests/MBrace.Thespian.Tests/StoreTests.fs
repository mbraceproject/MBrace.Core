namespace MBrace.Thespian.Tests

open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.Thespian

open MBrace.Runtime.Utils.XPlat

open NUnit.Framework

[<Category("ThespianClusterTests")>]
type ``MBrace Thespian FileStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop()

    override __.FileStore = session.Cluster.GetResource<ICloudFileStore>()
    override __.Serializer = session.Cluster.GetResource<ISerializer>()
    override __.IsCaseSensitive = 
        match currentPlatform.Value with
        | Platform.Linux | Platform.BSD | Platform.Unix -> true
        | _ -> false

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow


[<Category("ThespianClusterTests")>]
type ``MBrace Thespian CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
    override __.IsSupportedLevel _ = true


[<Category("ThespianClusterTests")>]
type ``MBrace Thespian Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsSupportedNamedLookup = false
    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow

#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

[<Category("ThespianClusterTests")>]
type ``MBrace Thespian Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
    override __.IsSupportedNamedLookup = false

[<Category("ThespianClusterTests")>]
type ``MBrace Thespian Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(workerCount = 4)

    [<OneTimeSetUp>]
    member __.Init () = session.Start()

    [<OneTimeTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsInMemoryFixture = false
    override __.IsSupportedNamedLookup = false
    override __.Run (workflow : Cloud<'T>) = session.Cluster.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally workflow
