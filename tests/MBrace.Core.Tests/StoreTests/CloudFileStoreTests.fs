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
type ``CloudFileStore Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf 
    let runOnCurrentProcess wf = self.RunLocally wf

    let testDirectory = lazy(self.FileStore.GetRandomDirectoryName())
    let runSync wf = Async.RunSync wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// FileStore implementation under test
    abstract FileStore : ICloudFileStore
    /// Serializer implementation under test
    abstract Serializer : ISerializer
    /// Specifies whether store implementation is expected to be case sensitive
    abstract IsCaseSensitive : bool

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow under local semantics in the test process
    abstract RunLocally : Cloud<'T> -> 'T

    //
    //  Section 1: Local raw fileStore tests
    //

    [<Test>]
    member self.``1. FileStore : UUID is not null or empty.`` () = 
        String.IsNullOrEmpty self.FileStore.Id
        |> shouldEqual false

    [<Test>]
    member self.``1. FileStore : check case sensitivity.`` () = 
        self.FileStore.IsCaseSensitiveFileSystem |> shouldEqual self.IsCaseSensitive

    [<Test>]
    member self.``1. FileStore : Store instance should be serializable`` () =
        let fileStore2 = self.Serializer.Clone self.FileStore
        fileStore2.Id |> shouldEqual self.FileStore.Id
        fileStore2.Name |> shouldEqual self.FileStore.Name

        // check that the cloned instance accesses the same store
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        do
            use stream = self.FileStore.BeginWrite file |> runSync
            for i = 1 to 100 do stream.WriteByte(byte i)

        fileStore2.FileExists file |> runSync |> shouldEqual true
        fileStore2.DeleteFile file |> runSync

        self.FileStore.FileExists file |> runSync |> shouldEqual false

    [<Test>]
    member self.``1. FileStore : Create and delete directory.`` () =
        let dir = self.FileStore.GetRandomDirectoryName()
        self.FileStore.DirectoryExists dir |> runSync |> shouldEqual false
        self.FileStore.CreateDirectory dir |> runSync
        self.FileStore.DirectoryExists dir |> runSync |> shouldEqual true
        self.FileStore.DeleteDirectory(dir, recursiveDelete = false) |> runSync
        self.FileStore.DirectoryExists dir |> runSync |> shouldEqual false

    [<Test>]
    member self.``1. FileStore : Get directory`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        file |> self.FileStore.GetDirectoryName |> shouldEqual testDirectory.Value

    [<Test>]
    member self.``1. FileStore : Get file name`` () =
        let name = "test.txt"
        let file = self.FileStore.Combine [|testDirectory.Value ; name |]
        file |> self.FileStore.GetDirectoryName |> shouldEqual testDirectory.Value
        file |> self.FileStore.GetFileName |> shouldEqual name

    [<Test>]
    member __.``1. FileStore : Enumerate root directories`` () =
        let directory = self.FileStore.GetRandomDirectoryName()
        self.FileStore.CreateDirectory directory |> runSync
        let directories = self.FileStore.EnumerateRootDirectories() |> runSync
        directories |> Array.exists((=) directory) |> shouldEqual true
        self.FileStore.DeleteDirectory(directory, recursiveDelete = false) |> runSync

    [<Test>]
    member self.``1. FileStore : Create, read and delete a file.`` () = 
        let file = self.FileStore.GetRandomFilePath testDirectory.Value

        self.FileStore.FileExists file |> runSync |> shouldEqual false

        // write to file
        do
            use stream = self.FileStore.BeginWrite file |> runSync
            for i = 1 to 100 do stream.WriteByte(byte i)

        self.FileStore.FileExists file |> runSync |> shouldEqual true
        self.FileStore.EnumerateFiles testDirectory.Value |> runSync |> Array.exists ((=) file) |> shouldEqual true

        // read from file
        do
            use stream = self.FileStore.BeginRead file |> runSync
            for i = 1 to 100 do
                stream.ReadByte() |> shouldEqual i

        self.FileStore.DeleteFile file |> runSync

        self.FileStore.FileExists file |> runSync |> shouldEqual false

    [<Test>]
    member self.``1. FileStore : simple etag verification`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value

        self.FileStore.TryGetETag file |> runSync |> shouldEqual None

        // write to file
        let writeEtag,_ = self.FileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        // get etag from file
        self.FileStore.TryGetETag file |> runSync |> shouldEqual (Some writeEtag)

        self.FileStore.DeleteFile file |> runSync

        self.FileStore.TryGetETag file |> runSync |> shouldEqual None

    [<Test>]
    member self.``1. FileStore : etag should change after file overwritten`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value

        self.FileStore.TryGetETag file |> runSync |> shouldEqual None

        // write to file
        let writeEtag,_ = self.FileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        self.FileStore.DeleteFile file |> runSync

        // write to file
        let writeEtag',_ = self.FileStore.WriteETag(file, fun stream -> async { do for i = 1 to 200 do stream.WriteByte(byte i) }) |> runSync

        Assert.AreNotEqual(writeEtag', writeEtag)

        self.FileStore.DeleteFile file |> runSync

    [<Test>]
    member self.``1. FileStore : Get byte count`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        // write to file
        let _ = self.FileStore.WriteETag(file, fun stream -> async { do for i = 1 to 100 do stream.WriteByte(byte i) }) |> runSync

        self.FileStore.GetFileSize file |> runSync |> shouldEqual 100L

        self.FileStore.DeleteFile file |> runSync

    [<Test>]
    member self.``1. FileStore : Create and Read a large file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        
        do
            use stream = self.FileStore.BeginWrite file |> runSync
            stream.Write(data, 0, data.Length)

        do
            use m = new MemoryStream()
            let stream = self.FileStore.BeginRead file |> runSync
            use stream = stream
            stream.CopyTo m
            m.ToArray() |> shouldEqual data
        
        self.FileStore.DeleteFile file |> runSync

    [<Test>]
    member self.``1. FileStore : from stream to file and back to stream.`` () =
        let data = Array.init (1024 * 1024) byte
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        do
            use m = new MemoryStream(data)
            let _ = self.FileStore.CopyOfStream(m, file) |> runSync
            ()

        do
            use m = new MemoryStream()
            let _ = self.FileStore.CopyToStream(file, m) |> runSync
            m.ToArray() |> shouldEqual data

        self.FileStore.DeleteFile file |> runSync

    [<Test>]
    member self.``1. FileStore : Concurrent writes to single path`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        let data = Array.init (1024 * 1024) byte
        let writeData _ = async {
            use! fs = self.FileStore.BeginWrite file
            do! fs.WriteAsync(data, 0, data.Length)
        }

        Seq.init 20 writeData |> Async.Parallel |> Async.Ignore |> Async.RunSync

        self.FileStore.GetFileSize file |> Async.RunSync |> shouldEqual (1024L * 1024L)

    [<Test>]
    member self.``1. FileStore : Concurrent reads to single path`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        let data = Array.init (1024 * 1024) byte
        do
            use stream = self.FileStore.BeginWrite file |> runSync
            stream.Write(data, 0, data.Length)

        let readData _ = async {
            use! fs = self.FileStore.BeginRead file
            let data = Array.zeroCreate<byte> (1024 * 1024)
            do! fs.ReadAsync(data, 0, data.Length)
            return data.Length
        }

        Seq.init 20 readData 
        |> Async.Parallel 
        |> Async.RunSync 
        |> Array.forall (fun l -> l = 1024 * 1024)
        |> shouldEqual true

    [<Test>]
    member self.``1. FileStore : Deleting non-existent path`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        self.FileStore.DeleteFile file |> Async.RunSync

        // test paths in non-existent directories
        let dir = self.FileStore.GetRandomDirectoryName()
        let file' = self.FileStore.GetRandomFilePath dir
        self.FileStore.DeleteFile file' |> Async.RunSync

    [<Test>]
    member self.``1. FileStore : Deleting non-existent directory`` () =
        let dir = self.FileStore.GetRandomDirectoryName()
        self.FileStore.DeleteDirectory(dir, false) |> Async.RunSync

    [<Test>]
    member self.``1. FileStore : Reading non-existent file should raise FileNotFoundException.`` () =
        let file = self.FileStore.GetRandomFilePath testDirectory.Value
        fun () -> self.FileStore.BeginRead file |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>

        fun () -> self.FileStore.GetFileSize file |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>

        fun () -> self.FileStore.GetLastModifiedTime (file, isDirectory = false) |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>

        // test paths in non-existent directories

        let dir = self.FileStore.GetRandomDirectoryName()
        let file' = self.FileStore.GetRandomFilePath dir
        
        fun () -> self.FileStore.BeginRead file' |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>

        fun () -> self.FileStore.GetFileSize file' |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>

        fun () -> self.FileStore.GetLastModifiedTime (file', isDirectory = false) |> Async.RunSync
        |> shouldFailwith<_, FileNotFoundException>


    [<Test>]
    member self.``1. FileStore : Reading non-existent directory should raise DirectoryNotFoundException.`` () =
        let dir = self.FileStore.GetRandomDirectoryName()
        fun () -> self.FileStore.EnumerateFiles dir |> Async.RunSync
        |> shouldFailwith<_, DirectoryNotFoundException>

        fun () -> self.FileStore.EnumerateFiles dir |> Async.RunSync
        |> shouldFailwith<_, DirectoryNotFoundException>

        fun () -> self.FileStore.GetLastModifiedTime(dir, isDirectory = true) |> Async.RunSync
        |> shouldFailwith<_, DirectoryNotFoundException>

    //
    //  Section 2. FileStore via MBrace runtime
    //

    [<Test>]
    member __.``2. MBrace : PersistedValue - simple`` () = 
        let ref = runOnCloud <| PersistedValue.New 42
        ref.Value |> shouldEqual 42

    [<Test>]
    member __.``2. MBrace : PersistedValue - should error if reading from changed persist file`` () =
        fun () ->
            cloud {
                let! c = PersistedValue.New [1..10000]
                do! CloudFile.Delete c.Path
                // overwrite persist file with payload of compatible type
                // cloudvalue should use etag implementation to infer that content has changed
                let! serializer = Cloud.GetResource<ISerializer> ()
                do! local {
                    use! stream = CloudFile.BeginWrite c.Path
                    do serializer.Serialize(stream, [1..100], false)
                }
                return c.Value
            } |> runOnCloud

        |> shouldFailwith<_,InvalidDataException>

    [<Test>]
    member __.``2. MBrace : PersistedValue - Parallel`` () =
        cloud {
            let! ref = PersistedValue.New [1 .. 100]
            let! (x, y) = cloud { return ref.Value.Length } <||> cloud { return ref.Value.Length }
            return x + y
        } |> runOnCloud |> shouldEqual 200

    [<Test>]
    member __.``2. MBrace : PersistedValue - Distributed tree`` () =
        let tree = CloudTree.createTree 5 |> runOnCloud
        CloudTree.getBranchCount tree |> runOnCloud |> shouldEqual 31


    [<Test>]
    member __.``2. MBrace : PersistedSequence - simple`` () = 
        let b = runOnCloud <| PersistedSequence.New [1..10000]
        b.Count |> shouldEqual 10000L
        b |> Seq.sum |> shouldEqual (List.sum [1..10000])
        b.ToArray() |> Array.sum |> shouldEqual (List.sum [1..10000])

    [<Test>]
    member __.``2. MBrace : PersistedSequence - should error if reading from changed persist file`` () =
        fun () ->
            cloud {
                let! c = PersistedSequence.New [1..10000]
                do! CloudFile.Delete c.Path
                let! serializer = Cloud.GetResource<ISerializer> ()
                // overwrite persist file with payload of compatible type
                // cloudsequence should use etag implementation to infer that content has changed
                do! local {
                    use! stream = CloudFile.BeginWrite c.Path
                    ignore <| serializer.SeqSerialize(stream, [1..100], false)
                }
   
                return c.ToArray()
            } |> runOnCloud

        |> shouldFailwith<_,InvalidDataException>

    [<Test>]
    member __.``2. MBrace : PersistedSequence - parallel`` () =
        let ref = runOnCloud <| PersistedSequence.New [1..10000]
        ref |> Seq.length |> shouldEqual 10000
        cloud {
            let! ref = PersistedSequence.New [1 .. 10000]
            let! (x, y) = 
                cloud { return Seq.length ref } 
                    <||>
                cloud { return Seq.length ref } 

            return x + y
        } |> runOnCloud |> shouldEqual 20000

    [<Test>]
    member __.``2. MBrace : PersistedSequence - partitioned`` () =
        cloud {
            let! seqs = PersistedSequence.NewPartitioned([|1L .. 1000000L|], 1024L * 1024L)
            seqs.Length |> shouldBe (fun l -> l >= 8 && l < 10)
            let! partialSums = seqs |> Array.map (fun c -> cloud {  return Seq.sum c }) |> Cloud.Parallel
            return Array.sum partialSums
        } |> runOnCloud |> shouldEqual (Array.sum [|1L .. 1000000L|])

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

            let! seq = PersistedSequence.OfCloudFile(file.Path, deserializer)
            let! ch = Cloud.StartChild(cloud { return Seq.length seq })
            return! ch
        } |> runOnCloud |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : PersistedSequence - read lines`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! file = CloudFile.WriteAllLines(path, [1..100] |> List.map (fun i -> string i))
            let! cseq = PersistedSequence.OfCloudFileByLine(file.Path)
            return Seq.length cseq
        } |> runOnCloud |> shouldEqual 100

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
                let! cseq = PersistedSequence.OfCloudFileByLine file.Path   
                return cseq :> ICloudCollection<string> :?> IPartitionableCollection<string>
            } |> runOnCurrentProcess

        let testPartitioning partitionCount =
            cloud {
                let! partitions = Cloud.OfAsync <| cseq.GetPartitions (Array.init partitionCount (fun _ -> 4))
                let! lines' = partitions |> Cloud.Balanced.collectLocal (fun c -> local { return! Cloud.OfAsync <| c.ToEnumerable() })
                lines'.Length |> shouldEqual 1000
                lines' |> Array.iteri check 
            } |> runOnCloud

        // AppVeyor has performance bottleneck when doing concurrent IO; reduce number of tests
        let testedPartitionCounts = 
            if isAppVeyorInstance then [|20|]
            else [|1;5;10;50;100;250;500;750;1000;2000|]

        for pc in testedPartitionCounts do
            testPartitioning pc


    [<Test>]
    member __.``2. MBrace : CloudFile - simple`` () =
        let path = CloudPath.GetRandomFileName() |> runOnCurrentProcess
        let file = CloudFile.WriteAllBytes(path, [|1uy .. 100uy|]) |> runOnCloud
        file.Size |> shouldEqual 100L
        cloud {
            let! bytes = CloudFile.ReadAllBytes file.Path
            return bytes.Length
        } |> runOnCloud |> shouldEqual 100

    [<Test>]
    member __.``2. MBrace : CloudFile - large`` () =
        let file =
            cloud {
                let text = Seq.init 1000 (fun _ -> "lorem ipsum dolor sit amet")
                let! path = CloudPath.GetRandomFileName()
                return! CloudFile.WriteAllLines(path, text)
            } |> runOnCloud

        cloud {
            let! lines = CloudFile.ReadLines file.Path
            return Seq.length lines
        } |> runOnCloud |> shouldEqual 1000

    [<Test>]
    member __.``2. MBrace : CloudFile - read from stream`` () =
        let mk a = Array.init (a * 1024) byte
        let n = 512
        cloud {
            let! path = CloudPath.GetRandomFileName()
            do! local {
                use! stream = CloudFile.BeginWrite path
                let b = mk n
                stream.Write(b, 0, b.Length)
                stream.Flush()
                stream.Dispose() 
            }

            return! CloudFile.ReadAllBytes path
        } |> runOnCloud |> shouldEqual (mk n)

    [<Test>]
    member __.``2. MBrace : CloudFile - get by name`` () =
        cloud {
            let! path = CloudPath.GetRandomFileName()
            use! f = CloudFile.WriteAllBytes(path, [|1uy..100uy|])
            let! t = Cloud.StartChild(CloudFile.ReadAllBytes f.Path)
            let! bytes = t
            return bytes
        } |> runOnCloud |> shouldEqual [|1uy .. 100uy|]

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
        } |> runOnCloud |> shouldEqual true

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to write on stream`` () =
        local {
            let! path = CloudPath.GetRandomFileName()
            do! local {
                use! stream = CloudFile.BeginWrite path
                stream.WriteByte(10uy) 
            }

            use! stream = CloudFile.BeginRead path
            stream.WriteByte(20uy)

        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``2. MBrace : CloudFile - attempt to read nonexistent file`` () =
        local {
            let! cf = CloudFile.GetInfo (Guid.NewGuid().ToString())
            use! stream = CloudFile.BeginRead cf.Path
            return stream.ReadByte()
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
        } |> runOnCloud

    [<Test>]
    member __.``2. MBrace : CloudDirectory - dispose`` () =
        let dir, file =
            cloud {
                let! dirPath = CloudPath.GetRandomDirectoryName()
                use! dir = CloudDirectory.Create dirPath
                let! path = CloudPath.GetRandomFileName dir.Path
                let! file = CloudFile.WriteAllText(path, "lorem ipsum dolor")
                return dir, file
            } |> runOnCloud

        CloudDirectory.Exists dir.Path |> runOnCurrentProcess |> shouldEqual false
        CloudFile.Exists file.Path |> runOnCurrentProcess |> shouldEqual false