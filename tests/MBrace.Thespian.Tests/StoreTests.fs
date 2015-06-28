namespace MBrace.Thespian.Tests

open System.Threading

open MBrace.Core
open MBrace.Core.Tests
open MBrace.Thespian

open NUnit.Framework

type ``MBrace Thespian FileStore Tests`` () =
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


type ``MBrace Thespian Atom Tests`` () =
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

type ``MBrace Thespian Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10)

    let session = new RuntimeSession(nodes = 4)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop ()

    override __.Run (workflow : Cloud<'T>) = session.Runtime.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = session.Runtime.RunLocally workflow
    override __.QueueClient = session.Runtime.StoreClient.Queue

type ``MBrace Thespian Dictionary Tests`` () =
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