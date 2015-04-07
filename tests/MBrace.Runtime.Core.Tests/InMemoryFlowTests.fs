namespace MBrace.Runtime.Tests

open MBrace
open MBrace.Store
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization
open MBrace.Runtime.Store
open MBrace.Flow.Tests

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    do VagabondRegistry.Initialize(throwOnError = false)

    let fileStore = FileSystemStore.CreateUniqueLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let objcache = InMemoryCache.Create()
    let fsConfig = CloudFileStoreConfiguration.Create(fileStore)
    let imem = MBrace.Client.LocalRuntime.Create(fileConfig = fsConfig, serializer = serializer, objectCache = objcache)

    override __.Run(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocal(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = 100