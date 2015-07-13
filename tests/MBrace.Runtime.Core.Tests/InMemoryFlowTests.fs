namespace MBrace.Runtime.Tests

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime.Vagabond
open MBrace.Runtime.InMemoryRuntime
open MBrace.Runtime.Store

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    // force Vagabond initialization
    let _ = Config.fsConfig

    let fileStore = FileSystemStore.CreateUniqueLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let fsConfig = CloudFileStoreConfiguration.Create(fileStore)
    let imem = InMemoryRuntime.Create(fileConfig = fsConfig, serializer = serializer, memoryMode = MemoryEmulation.Shared)

    override __.RunRemote(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30