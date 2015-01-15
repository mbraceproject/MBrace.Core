namespace MBrace.Store.Tests

open System
open System.IO

open MBrace
open MBrace.Store
open MBrace.Continuation

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``File Store Tests`` (fileStore : ICloudFileStore) =

    let run x = Async.RunSync x

    let testDirectory = fileStore.CreateUniqueDirectoryPath()

    [<Test>]
    member __.``UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.Id
        |> should equal false

    [<Test>]
    member __.``Store instance should be serializable`` () =
        let fileStore' = FsPickler.Clone fileStore
        fileStore'.Id |> should equal fileStore.Id
        fileStore'.Name |> should equal fileStore.Name

    [<Test>]
    member __.``Create and delete directory.`` () =
        let dir = fileStore.CreateUniqueDirectoryPath()
        fileStore.DirectoryExists dir |> run |> should equal false
        fileStore.CreateDirectory dir |> run
        fileStore.DirectoryExists dir |> run |> should equal true
        fileStore.DeleteDirectory(dir, recursiveDelete = false) |> run
        fileStore.DirectoryExists dir |> run |> should equal false

    [<Test>]
    member __.``Get directory`` () =
        let file = fileStore.GetRandomFilePath testDirectory
        file |> fileStore.GetDirectoryName |> should equal testDirectory

    [<Test>]
    member __.``Get file name`` () =
        let name = "test.txt"
        let file = fileStore.Combine [|testDirectory ; name |]
        file |> fileStore.GetDirectoryName |> should equal testDirectory
        file |> fileStore.GetFileName |> should equal name

    [<Test>]
    member __.``Enumerate root directories`` () =
        let directory = fileStore.CreateUniqueDirectoryPath()
        fileStore.CreateDirectory directory |> run
        let directories = fileStore.EnumerateRootDirectories() |> run
        directories |> Array.exists((=) directory) |> should equal true
        fileStore.DeleteDirectory(directory, recursiveDelete = false) |> run

    [<Test>]
    member test.``Create, read and delete a file.`` () = 
        let file = fileStore.GetRandomFilePath testDirectory

        fileStore.FileExists file |> run |> should equal false

        // write to file
        fileStore.Write(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> run


        fileStore.FileExists file |> run |> should equal true
        fileStore.EnumerateFiles testDirectory |> run |> Array.exists ((=) file) |> should equal true

        // read from file
        do
            use stream = fileStore.BeginRead file |> run
            for i = 1 to 100 do
                stream.ReadByte() |> should equal i

        fileStore.DeleteFile file |> run

        fileStore.FileExists file |> run |> should equal false

    [<Test>]
    member __.``Get byte count`` () =
        let file = fileStore.GetRandomFilePath testDirectory
        // write to file
        fileStore.Write(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> run

        fileStore.GetFileSize file |> run |> should equal 100

        fileStore.DeleteFile file |> run

    [<Test>]
    member test.``Create and Read a large file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let file = fileStore.GetRandomFilePath testDirectory
        
        fileStore.Write(file, fun stream -> async { stream.Write(data, 0, data.Length) }) |> run

        do
            use m = new MemoryStream()
            use stream = fileStore.BeginRead file |> run
            stream.CopyTo m
            m.ToArray() |> should equal data
        
        fileStore.DeleteFile file |> run

    [<Test>]
    member test.``from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = fileStore.GetRandomFilePath testDirectory
        do
            use m = new MemoryStream(data)
            fileStore.OfStream(m, file) |> run

        do
            use m = new MemoryStream()
            fileStore.ToStream(file, m) |> run
            m.ToArray() |> should equal data

        fileStore.DeleteFile file |> run

    [<TestFixtureTearDown>]
    member test.``Cleanup`` () =
        if fileStore.DirectoryExists testDirectory |> run then
            fileStore.DeleteDirectory(testDirectory, recursiveDelete = true) |> run