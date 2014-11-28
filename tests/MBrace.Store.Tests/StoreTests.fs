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

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

module StoreConfiguration =

    do VagrantRegistry.Initialize()

    let fileSystemStore = FileSystemStore.LocalTemp
    let serializer = FsPicklerStoreSerializer.Default
    do StoreRegistry.Register serializer

    let mkExecutionContext fileStore tableStoreOpt =
        let config =
            {
                FileStore = fileStore
                DefaultFileContainer = fileStore.CreateUniqueContainerName()
                TableStore = tableStoreOpt
                Serializer = serializer
            }

        resource { yield! InMemoryRuntime.Resource ; yield config }

[<TestFixture; AbstractClass>]
type ``File Store Tests`` (fileStore : ICloudFileStore) =
    do StoreRegistry.Register(fileStore, force = true)

    let run x = Async.RunSync x

    [<Literal>]
#if DEBUG
    let repeats = 10
#else
    let repeats = 3
#endif

    let testContainer = fileStore.CreateUniqueContainerName()

    [<Test>]
    member __.``A. UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.UUID
        |> should equal false

    [<Test>]
    member __.``A. Create and delete container.`` () =
        let container = fileStore.CreateUniqueContainerName()
        fileStore.ContainerExists container |> run |> should equal false
        fileStore.CreateContainer container |> run
        fileStore.ContainerExists container |> run |> should equal true
        fileStore.DeleteContainer container |> run
        fileStore.ContainerExists container |> run |> should equal false

    [<Test>]
    member __.``A. Get container`` () =
        let file = fileStore.CreateUniqueFileName testContainer
        file |> fileStore.GetFileContainer |> should equal testContainer

    [<Test>]
    member __.``A. Get file name`` () =
        let name = "test.txt"
        let file = fileStore.Combine(testContainer, name)
        file |> fileStore.GetFileContainer |> should equal testContainer
        file |> fileStore.GetFileName |> should equal name

    [<Test>]
    member __.``A. Enumerate containers`` () =
        let container = fileStore.CreateUniqueContainerName()
        fileStore.CreateContainer container |> run
        let containers = fileStore.EnumerateContainers() |> run
        containers |> Array.exists((=) container) |> should equal true
        fileStore.DeleteContainer container |> run

    [<Test>]
    member __.``A. Store factory should generate identical instances`` () =
        let fact = fileStore.GetFactory() |> FsPickler.Clone
        let fileStore' = fact.Create()
        fileStore'.UUID |> should equal fileStore.UUID

    [<Test>]
    member test.``A. Create, read and delete a file.`` () = 
        let file = fileStore.CreateUniqueFileName testContainer

        fileStore.FileExists file |> run |> should equal false

        // write to file
        do
            use stream = fileStore.BeginWrite file |> run
            for i = 1 to 100 do stream.WriteByte(byte i)

        fileStore.FileExists file |> run |> should equal true
        fileStore.EnumerateFiles testContainer |> run |> Array.exists ((=) file) |> should equal true

        // read from file
        do
            use stream = fileStore.BeginRead file |> run
            for i = 1 to 100 do
                stream.ReadByte() |> should equal i

        fileStore.DeleteFile file |> run

        fileStore.FileExists file |> run |> should equal false

    [<Test>]
    member __.``A. Get byte count`` () =
        let file = fileStore.CreateUniqueFileName testContainer
        // write to file
        do
            use stream = fileStore.BeginWrite file |> run
            for i = 1 to 100 do stream.WriteByte(byte i)

        fileStore.GetFileSize file |> run |> should equal 100

        fileStore.DeleteFile file |> run

    [<Test>]
    member test.``A. Create and Read a large file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let file = fileStore.CreateUniqueFileName testContainer
        do
            use stream = fileStore.BeginWrite file |> run
            stream.Write(data, 0, data.Length)

        do
            use m = new MemoryStream()
            use stream = fileStore.BeginRead file |> run
            stream.CopyTo m
            m.ToArray() |> should equal data
        
        fileStore.DeleteFile file |> run

    [<Test>]
    member test.``A. from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = fileStore.CreateUniqueFileName testContainer
        do
            use m = new MemoryStream(data)
            fileStore.OfStream(m, file) |> run

        do
            use m = new MemoryStream()
            fileStore.ToStream(file, m) |> run
            m.ToArray() |> should equal data
