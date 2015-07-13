namespace MBrace.Core.Tests

open System
open System.IO

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

#nowarn "444"

/// Cloud file store test suite
[<TestFixture; AbstractClass>]
type ``CloudFileStore Tests`` (fileStore : ICloudFileStore, serializer : ISerializer, parallelismFactor : int) as self =

    let runRemote wf = self.RunRemote wf 
    let runLocally wf = self.RunLocally wf

    let testDirectory = fileStore.GetRandomDirectoryName()
    let runSync wf = Async.RunSync wf

    let runProtected wf = 
        try self.RunRemote wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract RunRemote : Cloud<'T> -> 'T
    /// Evaluate workflow under local semantics in the test process
    abstract RunLocally : Cloud<'T> -> 'T

    [<TestFixtureTearDown>]
    member test.``FileStore Cleanup`` () =
        if fileStore.DirectoryExists testDirectory |> runSync then
            fileStore.DeleteDirectory(testDirectory, recursiveDelete = true) |> runSync

    //
    //  Section 1: Local raw fileStore tests
    //

    [<Test>]
    member __.``1. FileStore : UUID is not null or empty.`` () = 
        String.IsNullOrEmpty fileStore.Id
        |> shouldEqual false

    [<Test>]
    member __.``1. FileStore : Store instance should be serializable`` () =
        let fileStore' = serializer.Clone fileStore
        fileStore'.Id |> shouldEqual fileStore.Id
        fileStore'.Name |> shouldEqual fileStore.Name

        // check that the cloned instance accesses the same store
        let file = fileStore.GetRandomFilePath testDirectory
        do
            use stream = fileStore.BeginWrite file |> runSync
            for i = 1 to 100 do stream.WriteByte(byte i)

        fileStore'.FileExists file |> runSync |> shouldEqual true
        fileStore'.DeleteFile file |> runSync

        fileStore.FileExists file |> runSync |> shouldEqual false

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
        do
            use stream = fileStore.BeginWrite file |> runSync
            for i = 1 to 100 do stream.WriteByte(byte i)

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
    member __.``1. FileStore : simple etag verification`` () =
        let file = fileStore.GetRandomFilePath testDirectory

        fileStore.TryGetETag file |> runSync |> shouldEqual None

        // write to file
        let writeEtag,_ = fileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        // get etag from file
        fileStore.TryGetETag file |> runSync |> shouldEqual (Some writeEtag)

        fileStore.DeleteFile file |> runSync

        fileStore.TryGetETag file |> runSync |> shouldEqual None

    [<Test>]
    member __.``1. FileStore : etag should change after file overwritten`` () =
        let file = fileStore.GetRandomFilePath testDirectory

        fileStore.TryGetETag file |> runSync |> shouldEqual None

        // write to file
        let writeEtag,_ = fileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        fileStore.DeleteFile file |> runSync

        // write to file
        let writeEtag',_ = fileStore.WriteETag(file, fun stream -> async { do for i = 1 to 200 do stream.WriteByte(byte i) }) |> runSync

        Assert.AreNotEqual(writeEtag', writeEtag)

        fileStore.DeleteFile file |> runSync

    [<Test>]
    member __.``1. FileStore : Get byte count`` () =
        let file = fileStore.GetRandomFilePath testDirectory
        // write to file
        let _ = fileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        fileStore.GetFileSize file |> runSync |> shouldEqual 100L

        fileStore.DeleteFile file |> runSync

    [<Test>]
    member test.``1. FileStore : Create and Read a large file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let file = fileStore.GetRandomFilePath testDirectory
        
        do
            use stream = fileStore.BeginWrite file |> runSync
            stream.Write(data, 0, data.Length)

        do
            use m = new MemoryStream()
            let stream = fileStore.BeginRead file |> runSync
            use stream = stream
            stream.CopyTo m
            m.ToArray() |> shouldEqual data
        
        fileStore.DeleteFile file |> runSync

    [<Test>]
    member test.``1. FileStore : from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = fileStore.GetRandomFilePath testDirectory
        do
            use m = new MemoryStream(data)
            let _ = fileStore.CopyOfStream(m, file) |> runSync
            ()

        do
            use m = new MemoryStream()
            let _ = fileStore.CopyToStream(file, m) |> runSync
            m.ToArray() |> shouldEqual data

        fileStore.DeleteFile file |> runSync

    //
    //  Section 2. FileStore via MBrace runtime
    //

    [<Test>]
    member __.``2. MBrace : PersistedValue - simple`` () = 
        let ref = runRemote <| FilePersistedValue.New 42
        ref.Value |> shouldEqual 42

    [<Test>]
    member __.``2. MBrace : PersistedValue - should error if reading from changed persist file`` () =
        fun () ->
            cloud {
                let! c = FilePersistedValue.New [1..10000]
                do! CloudFile.Delete c.Path
                // overwrite persist file with payload of compatible type
                // cloudvalue should use etag implementation to infer that content has changed
                let! serializer = Cloud.GetResource<ISerializer> ()
                let! _ = CloudFile.Create(c.Path, fun s -> async { return serializer.Serialize(s, [1..100], false)})
                return c.Value
            } |> runRemote

        |> shouldFailwith<_,InvalidDataException>

    [<Test>]
    member __.``2. MBrace : PersistedValue - Parallel`` () =
        cloud {
            let! ref = FilePersistedValue.New [1 .. 100]
            let! (x, y) = cloud { return ref.Value.Length } <||> cloud { return ref.Value.Length }
            return x + y
        } |> runRemote |> shouldEqual 200

    [<Test>]
    member __.``2. MBrace : PersistedValue - Distributed tree`` () =
        let tree = CloudTree.createTree 5 |> runRemote
        CloudTree.getBranchCount tree |> runRemote |> shouldEqual 31


    [<Test>]
    member __.``2. MBrace : PersistedSequence - simple`` () = 
        let b = runRemote <| FilePersistedSequence.New [1..10000]
        b.Count |> shouldEqual 10000L
        b |> Seq.sum |> shouldEqual (List.sum [1..10000])
        b.ToArray() |> Array.sum |> shouldEqual (List.sum [1..10000])

    [<Test>]
    member __.``2. MBrace : PersistedSequence - should error if reading from changed persist file`` () =
        fun () ->
            cloud {
                let! c = FilePersistedSequence.New [1..10000]
                do! CloudFile.Delete c.Path
                let! serializer = Cloud.GetResource<ISerializer> ()
                // overwrite persist file with payload of compatible type
                // cloudsequence should use etag implementation to infer that content has changed
                let! _ = CloudFile.Create(c.Path, fun s -> async { return ignore <| serializer.SeqSerialize(s, [1..100], false)})
                return c.ToArray()
            } |> runRemote

        |> shouldFailwith<_,InvalidDataException>

    [<Test>]
    member __.``2. MBrace : PersistedSequence - parallel`` () =
        let ref = runRemote <| FilePersistedSequence.New [1..10000]
        ref |> Seq.length |> shouldEqual 10000
        cloud {
            let! ref = FilePersistedSequence.New [1 .. 10000]
            let! (x, y) = 
                cloud { return Seq.length ref } 
                    <||>
                cloud { return Seq.length ref } 

            return x + y
        } |> runRemote |> shouldEqual 20000

    [<Test>]
    member __.``2. MBrace : PersistedSequence - partitioned`` () =
        cloud {
            let! seqs = FilePersistedSequence.NewPartitioned([|1L .. 1000000L|], 1024L * 1024L)
            seqs.Length |> shouldBe (fun l -> l >= 8 && l < 10)
            let! partialSums = seqs |> Array.map (fun c -> cloud {  return Seq.sum c }) |> Cloud.Parallel
            return Array.sum partialSums
        } |> runRemote |> shouldEqual (Array.sum [|1L .. 1000000L|])

    [<Test>]
    member __.``2. MBrace : PersistedSequence - of deserializer`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! file = CloudFile.WriteAllLines(path, [1..100] |> List.map (fun i -> string i))
            let deserializer (s : System.IO.Stream) =
                seq {
                    use textReader = new System.IO.StreamReader(s)
                    while not textReader.EndOfStream do
                        yield textReader.ReadLine()
                }

            let! seq = FilePersistedSequence.OfCloudFile(file.Path, deserializer)
            let! ch = Cloud.StartChild(cloud { return Seq.length seq })
            return! ch
        } |> runRemote |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : PersistedSequence - read lines`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! file = CloudFile.WriteAllLines(path, [1..100] |> List.map (fun i -> string i))
            let! cseq = FilePersistedSequence.FromLineSeparatedTextFile(file.Path)
            return Seq.length cseq
        } |> runRemote |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : PersistedSequence - read lines partitioned`` () =
        let text = "lorem ipsum dolor sit amet consectetur adipiscing elit"
        let lines = Seq.init 1000 (fun i -> text.Substring(0, i % 41))
        let check i (line:string) = 
            if line.Length = i % 41 && text.StartsWith line then ()
            else
                raise <| new AssertionException(sprintf "unexpected line '%s' in position %d." line i)

        let cseq = 
            cloud {
                let! path = CloudPath.GetRandomFileName()
                let! file = CloudFile.WriteAllLines(path, lines)
                let! cseq = FilePersistedSequence.FromLineSeparatedTextFile file.Path   
                return cseq :> ICloudCollection<string> :?> IPartitionableCollection<string>
            } |> runLocally

        let testPartitioning partitionCount =
            cloud {
                let! partitions = cseq.GetPartitions (Array.init partitionCount (fun _ -> 4))
                let! lines' = partitions |> Cloud.Balanced.collectLocal (fun c -> local { return! c.ToEnumerable() })
                lines'.Length |> shouldEqual 1000
                lines' |> Array.iteri check 
            } |> runRemote

        // AppVeyor has performance bottleneck when doing concurrent IO; reduce number of tests
        let testedPartitionCounts = 
            if isAppVeyorInstance then [|20;2000|]
            else [|1;5;10;50;100;250;500;750;1000;2000|]

        for pc in testedPartitionCounts do
            testPartitioning pc


    [<Test>]
    member __.``2. MBrace : CloudFile - simple`` () =
        let path = CloudPath.GetRandomFileName() |> runLocally
        let file = CloudFile.WriteAllBytes(path, [|1uy .. 100uy|]) |> runRemote
        file.Size |> shouldEqual 100L
        cloud {
            let! bytes = CloudFile.ReadAllBytes file.Path
            return bytes.Length
        } |> runRemote |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : CloudFile - large`` () =
        let file =
            cloud {
                let text = Seq.init 1000 (fun _ -> "lorem ipsum dolor sit amet")
                let! path = CloudPath.GetRandomFileName()
                return! CloudFile.WriteAllLines(path, text)
            } |> runRemote

        cloud {
            let! lines = CloudFile.ReadLines file.Path
            return Seq.length lines
        } |> runRemote |> shouldEqual 1000

    [<Test>]
    member __.``2. MBrace : CloudFile - read from stream`` () =
        let mk a = Array.init (a * 1024) byte
        let n = 512
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! f = 
                CloudFile.Create(path, fun stream -> async {
                    let b = mk n
                    stream.Write(b, 0, b.Length)
                    stream.Flush()
                    stream.Dispose() })

            return! CloudFile.ReadAllBytes f.Path
        } |> runRemote |> shouldEqual (mk n)

    [<Test>]
    member __.``2. MBrace : CloudFile - get by name`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! f = CloudFile.WriteAllBytes(path, [|1uy..100uy|])
            let! t = Cloud.StartChild(CloudFile.ReadAllBytes f.Path)
            let! bytes = t
            return bytes
        } |> runRemote |> shouldEqual [|1uy .. 100uy|]

    [<Test>]
    member __.``2. MBrace : CloudFile - disposable`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            let! file = CloudFile.WriteAllText(path, "lorem ipsum dolor")
            do! cloud { use file = file in () }
            return! CloudFile.ReadAllText file.Path
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudFile - get files in container`` () =
        cloud {
            let! container = CloudPath.GetRandomDirectoryName()
            let! fileNames = CloudPath.Combine(container, Seq.map (sprintf "file%d") [1..10])
            let! files =
                fileNames
                |> Seq.map (fun f -> CloudFile.WriteAllBytes(f, [|1uy .. 100uy|]))
                |> Cloud.Parallel

            let! files' = CloudFile.Enumerate container
            return files.Length = files'.Length
        } |> runRemote |> shouldEqual true

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to write on stream`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! cf = CloudFile.Create(path, fun stream -> async { stream.WriteByte(10uy) })
            return! CloudFile.Read(cf.Path, fun stream -> async { stream.WriteByte(20uy) })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to read nonexistent file`` () =
        cloud {
            let! cf = CloudFile.FromPath (Guid.NewGuid().ToString())
            return! CloudFile.Read(cf.Path, fun s -> async { return s.ReadByte() })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudDirectory - Create; populate; delete`` () =
        cloud {
            let! dirPath = CloudPath.GetRandomDirectoryName()
            let! dir = CloudDirectory.Create dirPath
            let! exists = CloudDirectory.Exists dir.Path
            exists |> shouldEqual true
            let write i = cloud {
                let! path = CloudPath.GetRandomFileName dir.Path
                let! _ = CloudFile.WriteAllText(path, "lorem ipsum dolor")
                ()
            }

            do! Seq.init 20 write |> Cloud.Parallel |> Cloud.Ignore

            let! files = CloudFile.Enumerate dir.Path
            files.Length |> shouldEqual 20
            do! CloudDirectory.Delete(dir.Path, recursiveDelete = true)
            let! exists = CloudDirectory.Exists dir.Path
            exists |> shouldEqual false
        } |> runRemote

    [<Test>]
    member __.``2. MBrace : CloudDirectory - dispose`` () =
        let dir, file =
            cloud {
                let! dirPath = CloudPath.GetRandomDirectoryName()
                use! dir = CloudDirectory.Create dirPath
                let! path = CloudPath.GetRandomFileName dir.Path
                let! file = CloudFile.WriteAllText(path, "lorem ipsum dolor")
                return dir, file
            } |> runRemote

        CloudDirectory.Exists dir.Path |> runLocally |> shouldEqual false
        CloudFile.Exists file.Path |> runLocally |> shouldEqual false