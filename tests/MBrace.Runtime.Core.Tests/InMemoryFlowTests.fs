namespace MBrace.Runtime.Tests

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open MBrace.Flow.Tests

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    // force Vagabond initialization
    let _ = Config.fsConfig

    let fileStore = FileSystemStore.CreateUniqueLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let fsConfig = CloudFileStoreConfiguration.Create(fileStore)
    let imem = MBrace.Client.LocalRuntime.Create(fileConfig = fsConfig, serializer = serializer)

    override __.Run(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30