namespace MBrace.Runtime.Tests

open MBrace.Core
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization
open MBrace.Runtime.Store
open MBrace.Flow.Tests

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    do ignore Config.imem

    let fileStore = FileSystemStore.CreateUniqueLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let objcache = InMemoryCache.Create()
    let fsConfig = CloudFileStoreConfiguration.Create(fileStore)
    let imem = MBrace.Client.LocalRuntime.Create(fileConfig = fsConfig, serializer = serializer, objectCache = objcache)

    override __.Run(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 10