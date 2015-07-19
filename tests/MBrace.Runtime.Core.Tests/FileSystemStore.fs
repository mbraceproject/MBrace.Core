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
    let imem = InMemoryRuntime.Create(serializer = serializer, fileConfig = fsConfig, memoryMode = MemoryEmulation.Shared)

    override __.FileStore = fsConfig.FileStore
    override __.Serializer = serializer :> _
    override __.RunRemote(wf : Cloud<'T>) = imem.Run wf
    override __.RunLocally(wf : Cloud<'T>) = imem.Run wf


[<TestFixture>]
type ``Local FileSystemStore CloudValue Tests`` () =
    inherit ``CloudValue Tests``(parallelismFactor = 100)

    let fsStore = Config.fsStore :> ICloudFileStore
    let fsConfig = CloudFileStoreConfiguration.Create(Config.fsStore)
    let container = fsStore.GetRandomDirectoryName()
    let cloudValueProvider = StoreCloudValueProvider.InitCloudValueProvider(fsStore, container, serializer = Config.serializer, encapsulationThreshold = 1024L) :> ICloudValueProvider
    let imem = InMemoryRuntime.Create(serializer = Config.serializer, fileConfig = fsConfig, valueProvider = cloudValueProvider, memoryMode = MemoryEmulation.Copied)

    override __.RunRemote(wf : Cloud<'T>) = imem.Run wf
    override __.RunLocally(wf : Cloud<'T>) = imem.Run wf
    override __.IsSupportedLevel lvl = cloudValueProvider.IsSupportedStorageLevel lvl