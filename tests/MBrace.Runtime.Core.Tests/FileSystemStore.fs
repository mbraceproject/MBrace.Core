namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime
open MBrace.Core.Tests
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store

#nowarn "044"

[<AutoOpen>]
module private Config =
    do VagabondRegistry.Initialize(throwOnError = false)

    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore)

[<TestFixture>]
type ``FileSystemStore Tests`` () =
    inherit  ``Local FileStore Tests``(fsConfig, serializer)

[<TestFixture>]
type ``FileSystemStore Tests (cached)`` () =
    inherit  ``Local FileStore Tests``(fsConfig, serializer)