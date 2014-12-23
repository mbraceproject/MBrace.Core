namespace Nessos.MBrace.Store.Tests.FileSystem

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.Runtime.Vagrant
open Nessos.MBrace.Runtime.Store
open Nessos.MBrace.Runtime.InMemory
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store.Tests

[<AutoOpen>]
module private Config =
    do VagrantRegistry.Initialize(throwOnError = false)

    let fsStore = FileSystemStore.LocalTemp
    let atomProvider = FileSystemAtomProvider.LocalTemp
    let chanProvider = InMemoryChannelProvider()
    let serializer = VagrantRegistry.Serializer

[<TestFixture>]
type ``FileSystem File store tests`` () =
    inherit  ``File Store Tests``(fsStore)

[<TestFixture>]
type ``FileSystem Atom tests`` () =
    inherit  ``Atom Tests``(atomProvider)

[<TestFixture>]
type ``FileSystem MBrace tests`` () =
    inherit ``Local MBrace store tests``(fsStore, atomProvider, chanProvider, serializer)