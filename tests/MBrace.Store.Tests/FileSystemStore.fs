namespace MBrace.Store.Tests.FileSystem

open NUnit.Framework
open FsUnit

open MBrace
open MBrace.InMemory
open MBrace.Runtime.Vagrant
open MBrace.Runtime.Store
open MBrace.Continuation
open MBrace.Store.Tests

[<AutoOpen>]
module private Config =
    do VagrantRegistry.Initialize(throwOnError = false)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let atomProvider = FileSystemAtomProvider.Create(create = true, cleanup = false)
    let chanProvider = new InMemoryChannelProvider()
    let cache = InMemoryCache.Create()
    let serializer = VagrantRegistry.Serializer

[<TestFixture>]
type ``FileSystem File store tests`` () =
    inherit  ``File Store Tests``(fsStore)

[<TestFixture>]
type ``FileSystem Atom tests`` () =
    inherit  ``Atom Tests``(atomProvider)

[<TestFixture>]
type ``FileSystem MBrace tests`` () =
    inherit ``Local MBrace store tests``(fsStore, atomProvider, chanProvider, serializer, cache)