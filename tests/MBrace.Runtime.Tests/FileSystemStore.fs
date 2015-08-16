namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool

[<TestFixture>]
type ``Local FileSystemStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 100)

    let fsStore = Config.fsStore()
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = Config.serializer, memoryEmulation = MemoryEmulation.Copied)

    override __.FileStore = fsStore :> _
    override __.Serializer = Config.serializer :> _
    override __.RunOnCloud(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf


[<TestFixture>]
type ``Local FileSystemStore CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 100)

    let fsStore = Config.fsStore()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(fsStore, serializer = Config.serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = Config.serializer, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.RunOnCloud(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedLevel lvl = cloudValueProvider.IsSupportedStorageLevel lvl


[<TestFixture>]
type ``Local FileSystemStore CloudFlow Tests`` () =
    inherit ``CloudFlow tests``()

    let fsStore = Config.fsStore()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(mainStore = fsStore, serializer = Config.serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = Config.serializer, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.RunOnCloud(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedStorageLevel level = cloudValueProvider.IsSupportedStorageLevel level
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30