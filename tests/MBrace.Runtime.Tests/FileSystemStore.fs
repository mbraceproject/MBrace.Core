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

    let fsStore = FileSystemStore.CreateRandomLocal()
    let serializer = new ThreadPoolFsPicklerBinarySerializer()
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = serializer, memoryEmulation = MemoryEmulation.Copied)

    override __.FileStore = fsStore :> _
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = platformId = System.PlatformID.Unix
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf


[<TestFixture>]
type ``Local FileSystemStore CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 100)

    // StoreCloudValueProvider depends on Vagabond, ensure enabled
    do VagabondRegistry.Initialize(isClientSession = true)
    let fsStore = FileSystemStore.CreateRandomLocal()
    let serializer = new ThreadPoolFsPicklerBinarySerializer()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(fsStore, serializer = serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = serializer, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedLevel lvl = cloudValueProvider.IsSupportedStorageLevel lvl


[<TestFixture>]
type ``Local FileSystemStore CloudFlow Tests`` () =
    inherit ``CloudFlow tests``()
    
    // StoreCloudValueProvider depends on Vagabond, ensure enabled
    do VagabondRegistry.Initialize(isClientSession = true)
    let fsStore = FileSystemStore.CreateRandomLocal()
    let serializer = new ThreadPoolFsPicklerBinarySerializer()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(mainStore = fsStore, serializer = serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = ThreadPoolRuntime.Create(fileStore = fsStore, serializer = serializer, valueProvider = cloudValueProvider, memoryEmulation = MemoryEmulation.Copied)

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunOnCurrentProcess(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedStorageLevel level = cloudValueProvider.IsSupportedStorageLevel level
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30