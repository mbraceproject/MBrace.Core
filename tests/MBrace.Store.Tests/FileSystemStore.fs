namespace MBrace.Store.Tests

open NUnit.Framework

open MBrace
open MBrace.InMemory
open MBrace.Runtime.Vagrant
open MBrace.Runtime.Store
open MBrace.Store
open MBrace.Continuation
open MBrace.Tests

[<AutoOpen>]
module private Config =
    do VagrantRegistry.Initialize(throwOnError = false)

    let atomProvider = FileSystemAtomProvider.Create(create = true, cleanup = false)
    let fsStore = FileSystemStore.CreateSharedLocal()
    let serializer = VagrantRegistry.Serializer
    let config = CloudFileStoreConfiguration.Create(fsStore, serializer, cache = InMemoryCache.Create())

[<TestFixture>]
type ``FileSystem File store tests`` () =
    inherit  ``FileStore Tests``(fsStore, serializer, 100)

    let imem = InMemoryRuntime.Create(fileConfig = config)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.StoreClient = imem.StoreClient

[<TestFixture>]
type ``FileSystem Atom tests`` () =
    inherit  ``CloudAtom Tests``(100)

    let imem = InMemoryRuntime.Create(fileConfig = config)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.StoreClient = imem.StoreClient
