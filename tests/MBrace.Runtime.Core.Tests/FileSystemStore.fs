namespace MBrace.Runtime.Tests

open NUnit.Framework

open MBrace
open MBrace.Runtime.InMemory
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open MBrace.Store
open MBrace.Continuation
open MBrace.Tests

#nowarn "044"

[<AutoOpen>]
module private Config =
    do VagabondRegistry.Initialize(throwOnError = false)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore, VagabondRegistry.Serializer, cache = InMemoryCache.Create())

[<TestFixture>]
type ``FileSystemStore Tests`` () =
    inherit  ``Local FileStore Tests``({ fsConfig with Cache = None })
    override __.IsCachingStore = false

[<TestFixture>]
type ``FileSystemStore Tests (cached)`` () =
    inherit  ``Local FileStore Tests``(fsConfig)
    override __.IsCachingStore = true