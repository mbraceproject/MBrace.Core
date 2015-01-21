namespace MBrace.Store.Tests

open System
open System.Threading

open MBrace
open MBrace.Continuation
open MBrace.InMemory
open MBrace.Tests
open MBrace.Store
open MBrace.Store.Tests.TestTypes

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``MBrace store tests`` (?npar, ?nseq) as self =

    // number of parallel and sequential updates for CloudAtom tests.
    let npar = defaultArg npar 20
    let nseq = defaultArg nseq 10

    let runRemote wf = self.Run wf 
    let runLocal wf = self.RunLocal wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    abstract Run : Cloud<'T> * ?ct:CancellationToken -> 'T
    abstract RunLocal : Cloud<'T> -> 'T
    abstract StoreClient : StoreClient

    [<Test>]
    member __.``CloudRef - simple`` () = 
        let ref = runRemote <| CloudRef.New 42
        ref.Value |> runLocal |> should equal 42

    [<Test>]
    member __.``CloudRef - Parallel`` () =
        cloud {
            let! ref = CloudRef.New [1 .. 100]
            let! (x, y) = cloud { let! v = ref.Value in return v.Length } <||> cloud { let! v = ref.Value in return v.Length }
            return x + y
        } |> runRemote |> should equal 200

    [<Test>]
    member __.``CloudRef - Distributed tree`` () =
        let tree = createTree 5 |> runRemote
        getBranchCount tree |> runRemote |> should equal 31


    [<Test>]
    member __.``CloudSequence - simple`` () = 
        let b = runRemote <| CloudSequence.New [1..10000]
        b.Cache() |> runLocal |> should equal true
        b.Count |> runLocal |> should equal 10000
        b.ToEnumerable() |> runLocal |> Seq.sum |> should equal (List.sum [1..10000])

    [<Test>]
    member __.``CloudSequence - parallel`` () =
        let ref = runRemote <| CloudSequence.New [1..10000]
        ref.ToEnumerable() |> runLocal |> Seq.length |> should equal 10000
        cloud {
            let! ref = CloudSequence.New [1 .. 10000]
            let! (x, y) = 
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 
                    <||>
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 

            return x + y
        } |> runRemote |> should equal 20000

    [<Test>]
    member __.``CloudSequence - partitioned`` () =
        cloud {
            let! seqs = CloudSequence.NewPartitioned([|1L .. 1000000L|], 1024L * 1024L)
            seqs.Length |> should be (greaterThanOrEqualTo 8)
            seqs.Length |> should be (lessThan 10)
            let! partialSums = seqs |> Array.map (fun c -> cloud { let! e = c.ToEnumerable() in return Seq.sum e }) |> Cloud.Parallel
            return Array.sum partialSums
        } |> runRemote |> should equal (Array.sum [|1L .. 1000000L|])

    [<Test>]
    member __.``CloudSequence - of deserializer`` () =
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
        } |> runRemote |> should equal 100

    [<Test>]
    member __.``CloudFile - simple`` () =
        let file = CloudFile.WriteAllBytes [|1uy .. 100uy|] |> runRemote
        CloudFile.GetSize file |> runLocal |> should equal 100
        cloud {
            let! bytes = CloudFile.ReadAllBytes file
            return bytes.Length
        } |> runRemote |> should equal 100

    [<Test>]
    member __.``CloudFile - large`` () =
        let file =
            cloud {
                let text = Seq.init 1000 (fun _ -> "lorem ipsum dolor sit amet")
                return! CloudFile.WriteLines(text)
            } |> runRemote

        cloud {
            let! lines = CloudFile.ReadLines file
            return Seq.length lines
        } |> runRemote |> should equal 1000

    [<Test>]
    member __.``CloudFile - read from stream`` () =
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
        } |> runRemote |> should equal (mk n)

    [<Test>]
    member __.``CloudFile - get by name`` () =
        cloud {
            use! f = CloudFile.WriteAllBytes([|1uy..100uy|])
            let! t = Cloud.StartChild(cloud { 
                return! CloudFile.ReadAllBytes f.Path
            })

            return! t
        } |> runRemote |> should equal [|1uy .. 100uy|]

    [<Test>]
    member __.``CloudFile - disposable`` () =
        cloud {
            let! file = CloudFile.WriteAllText "lorem ipsum dolor"
            do! cloud { use file = file in () }
            return! CloudFile.ReadAllText file
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``CloudFile - get files in container`` () =
        cloud {
            let! container = FileStore.GetRandomDirectoryName()
            let! fileNames = FileStore.Combine(container, Seq.map (sprintf "file%d") [1..10])
            let! files =
                fileNames
                |> Seq.map (fun f -> CloudFile.WriteAllBytes([|1uy .. 100uy|], f))
                |> Cloud.Parallel

            let! files' = CloudFile.Enumerate container
            return files.Length = files'.Length
        } |> runRemote |> should equal true

    [<Test>]
    member __.``CloudFile - attempt to write on stream`` () =
        cloud {
            use! cf = CloudFile.Create(fun stream -> async { stream.WriteByte(10uy) })
            return! CloudFile.Read(cf, fun stream -> async { stream.WriteByte(20uy) })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``CloudFile - attempt to read nonexistent file`` () =
        cloud {
            let cf = new CloudFile(Guid.NewGuid().ToString())
            return! CloudFile.Read(cf, fun s -> async { return s.ReadByte() })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``CloudAtom - Sequential updates`` () =
        // avoid capturing test fixture class in closure
        let nseq = nseq
        let atom =
            cloud {
                let! a = CloudAtom.New 0
                for i in 1 .. 10 * nseq do
                    do! CloudAtom.Update (fun i -> i + 1) a

                return a
            } |> runRemote 
            
        atom.Value |> runLocal |> should equal (10 * nseq)

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - Parallel updates`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        let nseq = nseq
        let atom = 
            cloud {
                let! a = CloudAtom.New 0
                let worker _ = cloud {
                    for _ in 1 .. nseq do
                        do! CloudAtom.Update (fun i -> i + 1) a
                }
                do! Seq.init npar worker |> Cloud.Parallel |> Cloud.Ignore
                return a
            } |> runRemote 
        
        atom.Value |> runLocal |> should equal (npar * nseq)

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - Parallel updates with large obj`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        cloud {
            let! isSupported = CloudAtom.IsSupportedValue [1 .. 100]
            if isSupported then return true
            else
                let! atom = CloudAtom.New List.empty<int>
                do! Seq.init npar (fun i -> CloudAtom.Update (fun is -> i :: is) atom) |> Cloud.Parallel |> Cloud.Ignore
                let! values = atom.Value
                return List.sum values = List.sum [1..npar]
        } |> runRemote |> should equal true

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - transact with contention`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        cloud {
            let! a = CloudAtom.New 0
            let! results = Seq.init npar (fun _ -> CloudAtom.Transact(fun i -> i, (i+1)) a) |> Cloud.Parallel
            return Array.sum results
        } |> runRemote |> should equal (Array.sum [|0 .. npar - 1|])

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - force with contention`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        cloud {
            let! a = CloudAtom.New -1
            do! Seq.init npar (fun i -> CloudAtom.Force i a) |> Cloud.Parallel |> Cloud.Ignore
            return! a.Value
        } |> runRemote |> should be (greaterThanOrEqualTo 0)

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - dispose`` () =
        cloud {
            let! a = CloudAtom.New 0
            do! cloud { use a = a in () }
            return! CloudAtom.Read a
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``StoreClient - CloudFile`` () =
        let sc = __.StoreClient
        let lines = Seq.init 10 string
        let file = sc.Store.File.WriteLines(lines) |> Async.RunSynchronously
        sc.Store.File.ReadLines(file)
        |> Async.RunSynchronously
        |> should equal lines

    [<Test>]
    member __.``StoreClient - CloudAtom`` () =
        let sc = __.StoreClient
        let atom = sc.CloudAtom.New(41) 
        sc.CloudAtom.Update((+) 1) atom 
        sc.CloudAtom.Read atom
        |> should equal 42

    [<Test>]
    member __.``StoreClient - CloudChannel`` () =
        let sc = __.StoreClient
        let sp, rp = sc.CloudChannel.New() |> Async.RunSynchronously
        sc.CloudChannel.Send 42 sp |> Async.RunSynchronously
        sc.CloudChannel.Receive rp
        |> Async.RunSynchronously
        |> should equal 42

[<TestFixture; AbstractClass>]
type ``Local MBrace store tests`` (fileStore, atomProvider, channelProvider, serializer : ISerializer, cache, ?npar, ?nseq) =
    inherit ``MBrace store tests``(?npar = npar, ?nseq = nseq)

    let fileStoreConfig = CloudFileStoreConfiguration.Create(fileStore, serializer, cache = cache)
    let atomConfig = CloudAtomConfiguration.Create(atomProvider)
    let channelConfig = CloudChannelConfiguration.Create(channelProvider)

    let imem = InMemoryRuntime.Create(fileConfig = fileStoreConfig, atomConfig = atomConfig, channelConfig = channelConfig)

    let storeClient =
        let resources = resource {
            yield fileStoreConfig
            yield atomConfig
            yield channelConfig
        }
        StoreClient.CreateFromResources(resources)

    override __.Run(wf : Cloud<'T>, ?ct) = imem.Run(wf, ?cancellationToken = ct)
    override __.RunLocal(wf : Cloud<'T>) = imem.Run(wf)
    override __.StoreClient = storeClient
