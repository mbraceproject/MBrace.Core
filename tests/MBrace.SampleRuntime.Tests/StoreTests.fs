namespace MBrace.SampleRuntime.Tests

open System.Threading

open MBrace.Core
open MBrace.Core.Tests
open MBrace.SampleRuntime

open NUnit.Framework

type ``SampleRuntime FileStore Tests`` () =
    inherit ``FileStore Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally workflow
    override __.StoreClient = session.Runtime.StoreClient
    override __.IsObjectCacheInstalled = true


type ``SampleRuntime Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally workflow
    override __.AtomClient = session.Runtime.StoreClient.Atom
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

type ``SampleRuntime Channel Tests`` () =
    inherit ``CloudChannel Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally workflow
    override __.ChannelClient = session.Runtime.StoreClient.Channel

type ``SampleRuntime Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.IsInMemoryFixture = false
    override __.Run (workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally workflow
    override __.DictionaryClient = session.Runtime.StoreClient.Dictionary