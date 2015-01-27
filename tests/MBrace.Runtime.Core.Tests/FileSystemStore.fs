namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace
open MBrace.Runtime.InMemory
open MBrace.Runtime.Vagrant
open MBrace.Runtime.Store
open MBrace.Store
open MBrace.Continuation
open MBrace.Tests

#nowarn "044"

[<AutoOpen>]
module private Config =
    do VagrantRegistry.Initialize(throwOnError = false)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore, VagrantRegistry.Serializer, cache = InMemoryCache.Create())

    let atomProvider = FileSystemAtomProvider.Create(create = true, cleanup = false) 
    let atomConfig = CloudAtomConfiguration.Create(atomProvider)

[<TestFixture>]
type ``FileSystemStore Tests`` () =
    inherit  ``Local FileStore Tests``({ fsConfig with Cache = None })
    override __.IsCachingStore = false

[<TestFixture>]
type ``FileSystemStore Tests (cached)`` () =
    inherit  ``Local FileStore Tests``(fsConfig)
    override __.IsCachingStore = true

[<TestFixture>]
type ``FileSystem Atom tests`` () =
    inherit  ``CloudAtom Tests``(nParallel = 20)
    let imem = InMemoryRuntime.Create(atomConfig = atomConfig)

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.AtomClient = imem.StoreClient.Atom
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif