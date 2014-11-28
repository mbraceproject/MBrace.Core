namespace Nessos.MBrace.Store.Tests

open System
open System.IO

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``File Store Tests`` (fileStore : ICloudFileStore) =
    do StoreRegistry.Register(fileStore, force = true)

    let run x = Async.RunSync x

    let testContainer = fileStore.CreateUniqueContainerName()

    [<Test>]
    member __.``UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.UUID
        |> should equal false

    [<Test>]
    member __.``Store factory should generate identical instances`` () =
        let fact = fileStore.GetFactory() |> FsPickler.Clone
        let fileStore' = fact.Create()
        fileStore'.UUID |> should equal fileStore.UUID

    [<Test>]
    member __.``Create and delete container.`` () =
        let container = fileStore.CreateUniqueContainerName()
        fileStore.ContainerExists container |> run |> should equal false
        fileStore.CreateContainer container |> run
        fileStore.ContainerExists container |> run |> should equal true
        fileStore.DeleteContainer container |> run
        fileStore.ContainerExists container |> run |> should equal false

    [<Test>]
    member __.``Get container`` () =
        let file = fileStore.CreateUniqueFileName testContainer
        file |> fileStore.GetFileContainer |> should equal testContainer

    [<Test>]
    member __.``Get file name`` () =
        let name = "test.txt"
        let file = fileStore.Combine(testContainer, name)
        file |> fileStore.GetFileContainer |> should equal testContainer
        file |> fileStore.GetFileName |> should equal name

    [<Test>]
    member __.``Enumerate containers`` () =
        let container = fileStore.CreateUniqueContainerName()
        fileStore.CreateContainer container |> run
        let containers = fileStore.EnumerateContainers() |> run
        containers |> Array.exists((=) container) |> should equal true
        fileStore.DeleteContainer container |> run

    [<Test>]
    member test.``Create, read and delete a file.`` () = 
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
    member __.``Get byte count`` () =
        let file = fileStore.CreateUniqueFileName testContainer
        // write to file
        do
            use stream = fileStore.BeginWrite file |> run
            for i = 1 to 100 do stream.WriteByte(byte i)

        fileStore.GetFileSize file |> run |> should equal 100

        fileStore.DeleteFile file |> run

    [<Test>]
    member test.``Create and Read a large file.`` () =
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
    member test.``from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = fileStore.CreateUniqueFileName testContainer
        do
            use m = new MemoryStream(data)
            fileStore.OfStream(m, file) |> run

        do
            use m = new MemoryStream()
            fileStore.ToStream(file, m) |> run
            m.ToArray() |> should equal data
