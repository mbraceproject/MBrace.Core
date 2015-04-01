namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace
open MBrace.Runtime.InMemory
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization
open MBrace.Runtime.Store
open MBrace.Store
open MBrace.Continuation
open MBrace.Tests

#nowarn "044"

[<AutoOpen>]
module private Config =
    do VagabondRegistry.Initialize(throwOnError = false)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let imem = InMemoryCache.Create()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore, serializer)

[<TestFixture>]
type ``FileSystemStore Tests`` () =
    inherit  ``Local FileStore Tests``(fsConfig, ?objectCache = None)

[<TestFixture>]
type ``FileSystemStore Tests (cached)`` () =
    inherit  ``Local FileStore Tests``(fsConfig, objectCache = imem)