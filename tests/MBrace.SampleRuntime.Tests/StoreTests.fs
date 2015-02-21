namespace MBrace.SampleRuntime.Tests

open System.Threading

open MBrace
open MBrace.Tests
open MBrace.SampleRuntime

open NUnit.Framework

type ``SampleRuntime FileStore Tests`` () =
    inherit ``FileStore Tests``(nParallel = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Workflow<'T>) = session.Runtime.Run workflow
    override __.RunLocal(workflow : Workflow<'T>) = session.Runtime.RunLocal workflow
    override __.FileStoreClient = session.Runtime.StoreClient.FileStore
    override __.IsCachingStore = true


type ``SampleRuntime Atom Tests`` () =
    inherit ``CloudAtom Tests``(nParallel = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Workflow<'T>) = session.Runtime.Run workflow
    override __.RunLocal(workflow : Workflow<'T>) = session.Runtime.RunLocal workflow
    override __.AtomClient = session.Runtime.StoreClient.Atom
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

type ``SampleRuntime Channel Tests`` () =
    inherit ``CloudChannel Tests``(nParallel = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Workflow<'T>) = session.Runtime.Run workflow
    override __.RunLocal(workflow : Workflow<'T>) = session.Runtime.RunLocal workflow
    override __.ChannelClient = session.Runtime.StoreClient.Channel