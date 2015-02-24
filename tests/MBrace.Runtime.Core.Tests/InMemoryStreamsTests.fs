namespace MBrace.Runtime.Tests

open MBrace
open MBrace.Store
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization
open MBrace.Runtime.Store
open MBrace.Streams.Tests

type ``InMemory CloudStreams tests`` () =
    inherit ``CloudStreams tests`` ()

    do VagabondRegistry.Initialize(throwOnError = false)

    let fileStore = FileSystemStore.CreateUniqueLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let objcache = InMemoryCache.Create()
    let fsConfig = CloudFileStoreConfiguration.Create(fileStore, serializer, cache = objcache)
    let imem = MBrace.Client.LocalRuntime.Create(fileConfig = fsConfig)

    override __.Run(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocal(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = 100