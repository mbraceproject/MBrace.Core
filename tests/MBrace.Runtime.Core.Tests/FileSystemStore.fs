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
    inherit ``CloudFileStore Tests``(Config.fsStore, Config.serializer, parallelismFactor = 100)

    let fsConfig = CloudFileStoreConfiguration.Create(Config.fsStore)
    let imem = InMemoryRuntime.Create(serializer = Config.serializer, fileConfig = fsConfig, memoryMode = MemoryEmulation.Shared)

    override __.RunRemote(wf : Cloud<'T>) = imem.Run wf
    override __.RunLocally(wf : Cloud<'T>) = imem.Run wf