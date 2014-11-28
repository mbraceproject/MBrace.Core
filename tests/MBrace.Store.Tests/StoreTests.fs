namespace Nessos.MBrace.Store.Tests

open System
open System.IO
open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Serialization
open Nessos.MBrace.Runtime.Store
open Nessos.MBrace.Tests

open NUnit.Framework
open FsUnit

module StoreConfiguration =

    do VagrantRegistry.Initialize()

    let fileSystemStore = FileSystemStore.LocalTemp

    let mkExecutionContext fileStore tableStoreOpt =
        let config =
            {
                FileStore = fileStore
                DefaultFileContainer = fileStore.CreateUniqueContainerName()
                TableStore = tableStoreOpt
                Serializer = FsPicklerStoreSerializer.Default
            }

        resource { yield! InMemoryRuntime.Resource ; yield config }

[<TestFixture; AbstractClass>]
type ``File Store Tests`` (fileStore : ICloudFileStore) =

    let run x = Async.RunSync x

    [<Literal>]
#if DEBUG
    let repeats = 10
#else
    let repeats = 3
#endif

    [<Test>]
    member __.``A.0 UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.UUID
        |> should equal false

    [<Test>]
    member __.``A.1 Create and delete container.`` () =
        let container = fileStore.CreateUniqueContainerName()
        fileStore.ContainerExists container |> run |> should equal false
        fileStore.CreateContainer container |> run
        fileStore.ContainerExists container |> run |> should equal true
        fileStore.DeleteContainer container |> run
        fileStore.ContainerExists container |> run |> should equal false