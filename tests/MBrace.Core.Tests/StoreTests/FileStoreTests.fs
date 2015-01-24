namespace MBrace.Tests

open System
open System.IO

open NUnit.Framework

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.InMemory

/// Cloud file store test suite
[<TestFixture; AbstractClass>]
type ``FileStore Tests`` (nParallel : int) as self =

    let runRemote wf = self.Run wf 
    let runLocal wf = self.RunLocal wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocal : Cloud<'T> -> 'T
    /// Store client to be tested
    abstract FileStoreClient : FileStoreClient
    /// denotes that underlying store employs caching
    abstract IsCachingStore : bool

    //
    //  Section 2. FileStore via MBrace runtime
    //


    [<Test>]
    member __.``2. MBrace : CloudRef - simple`` () = 
        let ref = runRemote <| CloudRef.New 42
        ref.Value |> runLocal |> shouldEqual 42

    [<Test>]
    member __.``2. MBrace : CloudRef - caching`` () = 
        if __.IsCachingStore then
            let b = runRemote <| CloudRef.New [1..10000]
            b.Cache() |> runLocal |> shouldEqual true
            let a1 = b.Value |> runLocal
            let a2 = b.Value |> runLocal
            obj.ReferenceEquals(a1, a2) |> shouldEqual true

    [<Test>]
    member __.``2. MBrace : CloudRef - Parallel`` () =
        cloud {
            let! ref = CloudRef.New [1 .. 100]
            let! (x, y) = cloud { let! v = ref.Value in return v.Length } <||> cloud { let! v = ref.Value in return v.Length }
            return x + y
        } |> runRemote |> shouldEqual 200

    [<Test>]
    member __.``2. MBrace : CloudRef - Distributed tree`` () =
        let tree = CloudTree.createTree 5 |> runRemote
        CloudTree.getBranchCount tree |> runRemote |> shouldEqual 31


    [<Test>]
    member __.``2. MBrace : CloudSequence - simple`` () = 
        let b = runRemote <| CloudSequence.New [1..10000]
        b.Count |> runLocal |> shouldEqual 10000
        b.ToEnumerable() |> runLocal |> Seq.sum |> shouldEqual (List.sum [1..10000])
        b.ToArray() |> runLocal |> Array.sum |> shouldEqual (List.sum [1..10000])

    [<Test>]
    member __.``2. MBrace : CloudSequence - caching`` () = 
        if __.IsCachingStore then
            let b = runRemote <| CloudSequence.New [1..10000]
            b.Cache() |> runLocal |> shouldEqual true
            let a1 = b.ToArray() |> runLocal
            let a2 = b.ToArray() |> runLocal
            obj.ReferenceEquals(a1, a2) |> shouldEqual true

    [<Test>]
    member __.``2. MBrace : CloudSequence - parallel`` () =
        let ref = runRemote <| CloudSequence.New [1..10000]
        ref.ToEnumerable() |> runLocal |> Seq.length |> shouldEqual 10000
        cloud {
            let! ref = CloudSequence.New [1 .. 10000]
            let! (x, y) = 
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 
                    <||>
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 

            return x + y
        } |> runRemote |> shouldEqual 20000

    [<Test>]
    member __.``2. MBrace : CloudSequence - partitioned`` () =
        cloud {
            let! seqs = CloudSequence.NewPartitioned([|1L .. 1000000L|], 1024L * 1024L)
            seqs.Length |> shouldBe (fun l -> l >= 8 && l < 10)
            let! partialSums = seqs |> Array.map (fun c -> cloud { let! e = c.ToEnumerable() in return Seq.sum e }) |> Cloud.Parallel
            return Array.sum partialSums
        } |> runRemote |> shouldEqual (Array.sum [|1L .. 1000000L|])

    [<Test>]
    member __.``2. MBrace : CloudSequence - of deserializer`` () =
        cloud {
            use! file = CloudFile.WriteLines([1..100] |> List.map (fun i -> string i))
            let deserializer (s : System.IO.Stream) =
                seq {
                    use textReader = new System.IO.StreamReader(s)
                    while not textReader.EndOfStream do
                        yield textReader.ReadLine()
                }

            let! seq = CloudSequence.FromFile(file.Path, deserializer)
            let! ch = Cloud.StartChild(cloud { let! e = seq.ToEnumerable() in return Seq.length e })
            return! ch
        } |> runRemote |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : CloudFile - simple`` () =
        let file = CloudFile.WriteAllBytes [|1uy .. 100uy|] |> runRemote
        CloudFile.GetSize file |> runLocal |> shouldEqual 100L
        cloud {
            let! bytes = CloudFile.ReadAllBytes file
            return bytes.Length
        } |> runRemote |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : CloudFile - large`` () =
        let file =
            cloud {
                let text = Seq.init 1000 (fun _ -> "lorem ipsum dolor sit amet")
                return! CloudFile.WriteLines(text)
            } |> runRemote

        cloud {
            let! lines = CloudFile.ReadLines file
            return Seq.length lines
        } |> runRemote |> shouldEqual 1000

    [<Test>]
    member __.``2. MBrace : CloudFile - read from stream`` () =
        let mk a = Array.init (a * 1024) byte
        let n = 512
        cloud {
            use! f = 
                CloudFile.Create(fun stream -> async {
                    let b = mk n
                    stream.Write(b, 0, b.Length)
                    stream.Flush()
                    stream.Dispose() })

            let! bytes = CloudFile.ReadAllBytes(f)
            return bytes
        } |> runRemote |> shouldEqual (mk n)

    [<Test>]
    member __.``2. MBrace : CloudFile - get by name`` () =
        cloud {
            use! f = CloudFile.WriteAllBytes([|1uy..100uy|])
            let! t = Cloud.StartChild(cloud { 
                return! CloudFile.ReadAllBytes f.Path
            })

            return! t
        } |> runRemote |> shouldEqual [|1uy .. 100uy|]

    [<Test>]
    member __.``2. MBrace : CloudFile - disposable`` () =
        cloud {
            let! file = CloudFile.WriteAllText "lorem ipsum dolor"
            do! cloud { use file = file in () }
            return! CloudFile.ReadAllText file
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudFile - get files in container`` () =
        cloud {
            let! container = FileStore.GetRandomDirectoryName()
            let! fileNames = FileStore.Combine(container, Seq.map (sprintf "file%d") [1..10])
            let! files =
                fileNames
                |> Seq.map (fun f -> CloudFile.WriteAllBytes([|1uy .. 100uy|], f))
                |> Cloud.Parallel

            let! files' = CloudFile.Enumerate container
            return files.Length = files'.Length
        } |> runRemote |> shouldEqual true

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to write on stream`` () =
        cloud {
            use! cf = CloudFile.Create(fun stream -> async { stream.WriteByte(10uy) })
            return! CloudFile.Read(cf, fun stream -> async { stream.WriteByte(20uy) })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to read nonexistent file`` () =
        cloud {
            let cf = new CloudFile(Guid.NewGuid().ToString())
            return! CloudFile.Read(cf, fun s -> async { return s.ReadByte() })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudDirectory - Create; populate; delete`` () =
        cloud {
            let! dir = CloudDirectory.Create ()
            let! exists = CloudDirectory.Exists dir
            exists |> shouldEqual true
            let write i = cloud {
                let! path = FileStore.GetRandomFileName dir
                let! _ = CloudFile.WriteAllText("lorem ipsum dolor", path = path)
                ()
            }

            do! Seq.init 20 write |> Cloud.Parallel |> Cloud.Ignore

            let! files = CloudFile.Enumerate dir
            files.Length |> shouldEqual 20
            do! CloudDirectory.Delete(dir, recursiveDelete = true)
            let! exists = CloudDirectory.Exists dir
            exists |> shouldEqual false
        } |> runRemote

    [<Test>]
    member __.``2. MBrace : CloudDirectory - dispose`` () =
        let dir, file =
            cloud {
                use! dir = CloudDirectory.Create ()
                let! path = FileStore.GetRandomFileName dir
                let! file = CloudFile.WriteAllText("lorem ipsum dolor", path = path)
                return dir, file
            } |> runRemote

        CloudDirectory.Exists dir |> runLocal |> shouldEqual false
        CloudFile.Exists file |> runLocal |> shouldEqual false


/// Cloud file store test suite
[<TestFixture; AbstractClass>]
type ``Local FileStore Tests`` (config : CloudFileStoreConfiguration) =
    inherit ``FileStore Tests`` (nParallel = 100)

    let imem = InMemoryRuntime.Create(fileConfig = config)

    let fileStore = config.FileStore
    let testDirectory = fileStore.GetRandomDirectoryName()
    let runSync wf = Async.RunSync wf

    override __.Run wf = imem.Run wf
    override __.RunLocal wf = imem.Run wf
    override __.FileStoreClient = imem.StoreClient.FileStore

    //
    //  Section 1: Local raw fileStore tests
    //

    [<Test>]
    member __.``1. FileStore : UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.Id
        |> shouldEqual false

    [<Test>]
    member __.``1. FileStore : Store instance should be serializable`` () =
        let fileStore' = config.Serializer.Clone fileStore
        fileStore'.Id |> shouldEqual fileStore.Id
        fileStore'.Name |> shouldEqual fileStore.Name

    [<Test>]
    member __.``1. FileStore : Create and delete directory.`` () =
        let dir = fileStore.GetRandomDirectoryName()
        fileStore.DirectoryExists dir |> runSync |> shouldEqual false
        fileStore.CreateDirectory dir |> runSync
        fileStore.DirectoryExists dir |> runSync |> shouldEqual true
        fileStore.DeleteDirectory(dir, recursiveDelete = false) |> runSync
        fileStore.DirectoryExists dir |> runSync |> shouldEqual false

    [<Test>]
    member __.``1. FileStore : Get directory`` () =
        let file = fileStore.GetRandomFilePath testDirectory
        file |> fileStore.GetDirectoryName |> shouldEqual testDirectory

    [<Test>]
    member __.``1. FileStore : Get file name`` () =
        let name = "test.txt"
        let file = fileStore.Combine [|testDirectory ; name |]
        file |> fileStore.GetDirectoryName |> shouldEqual testDirectory
        file |> fileStore.GetFileName |> shouldEqual name

    [<Test>]
    member __.``1. FileStore : Enumerate root directories`` () =
        let directory = fileStore.GetRandomDirectoryName()
        fileStore.CreateDirectory directory |> runSync
        let directories = fileStore.EnumerateRootDirectories() |> runSync
        directories |> Array.exists((=) directory) |> shouldEqual true
        fileStore.DeleteDirectory(directory, recursiveDelete = false) |> runSync

    [<Test>]
    member test.``1. FileStore : Create, read and delete a file.`` () = 
        let file = fileStore.GetRandomFilePath testDirectory

        fileStore.FileExists file |> runSync |> shouldEqual false

        // write to file
        fileStore.Write(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync


        fileStore.FileExists file |> runSync |> shouldEqual true
        fileStore.EnumerateFiles testDirectory |> runSync |> Array.exists ((=) file) |> shouldEqual true

        // read from file
        do
            use stream = fileStore.BeginRead file |> runSync
            for i = 1 to 100 do
                stream.ReadByte() |> shouldEqual i

        fileStore.DeleteFile file |> runSync

        fileStore.FileExists file |> runSync |> shouldEqual false

    [<Test>]
    member __.``1. FileStore : Get byte count`` () =
        let file = fileStore.GetRandomFilePath testDirectory
        // write to file
        fileStore.Write(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        fileStore.GetFileSize file |> runSync |> shouldEqual 100L

        fileStore.DeleteFile file |> runSync

    [<Test>]
    member test.``1. FileStore : Create and Read a large file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let file = fileStore.GetRandomFilePath testDirectory
        
        fileStore.Write(file, fun stream -> async { stream.Write(data, 0, data.Length) }) |> runSync

        do
            use m = new MemoryStream()
            use stream = fileStore.BeginRead file |> runSync
            stream.CopyTo m
            m.ToArray() |> shouldEqual data
        
        fileStore.DeleteFile file |> runSync

    [<Test>]
    member test.``1. FileStore : from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = fileStore.GetRandomFilePath testDirectory
        do
            use m = new MemoryStream(data)
            fileStore.OfStream(m, file) |> runSync

        do
            use m = new MemoryStream()
            fileStore.ToStream(file, m) |> runSync
            m.ToArray() |> shouldEqual data

        fileStore.DeleteFile file |> runSync

    [<Test>]
    member __.``1. FileStore : StoreClient - CloudFile`` () =
        let sc = __.FileStoreClient
        let lines = Array.init 10 string
        let file = sc.File.WriteLines(lines)
        sc.File.ReadLines(file)
        |> shouldEqual lines

    [<TestFixtureTearDown>]
    member test.``FileStore Cleanup`` () =
        if fileStore.DirectoryExists testDirectory |> runSync then
            fileStore.DeleteDirectory(testDirectory, recursiveDelete = true) |> runSync
