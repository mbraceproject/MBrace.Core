namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open MBrace.Runtime.InMemoryRuntime

[<TestFixture>]
type ``Local FileSystemStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 100)

    let fsConfig = CloudFileStoreConfiguration.Create(Config.fsStore)
    let serializer = Config.serializer
    let imem = InMemoryRuntime.Create(serializer = serializer, fileConfig = fsConfig, memoryEmulation = MemoryEmulation.Copied)

    override __.FileStore = fsConfig.FileStore
    override __.Serializer = serializer :> _
    override __.RunOnCloud(wf : Cloud<'T>) = imem.Run wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.Run wf


[<TestFixture>]
type ``Local FileSystemStore CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 100)

    let fsStore = Config.fsStore :> ICloudFileStore
    let fsConfig = CloudFileStoreConfiguration.Create(Config.fsStore)
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(fsConfig, serializer = Config.serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = InMemoryRuntime.Create(serializer = Config.serializer, fileConfig = fsConfig, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.RunOnCloud(wf : Cloud<'T>) = imem.Run wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.Run wf
    override __.IsSupportedLevel lvl = cloudValueProvider.IsSupportedStorageLevel lvl


[<TestFixture>]
type ``Local FileSystemStore CloudFlow Tests`` () =
    inherit ``CloudFlow tests``()

    let fsStore = Config.fsStore :> ICloudFileStore
    let fsConfig = CloudFileStoreConfiguration.Create(Config.fsStore)
    let container = fsStore.GetRandomDirectoryName()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(fsConfig, serializer = Config.serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = InMemoryRuntime.Create(serializer = Config.serializer, fileConfig = fsConfig, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.RunOnCloud(wf : Cloud<'T>) = imem.Run wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.Run wf
    override __.IsSupportedStorageLevel level = cloudValueProvider.IsSupportedStorageLevel level
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30