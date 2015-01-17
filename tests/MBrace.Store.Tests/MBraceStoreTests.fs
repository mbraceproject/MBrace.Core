namespace MBrace.Store.Tests

open System
open System.Threading

open MBrace
open MBrace.Continuation
open MBrace.Runtime.InMemory
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

    let run wf = self.Run wf 
    let runLocal wf = self.RunLocal wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    abstract Run : Cloud<'T> * ?ct:CancellationToken -> 'T
    abstract RunLocal : Cloud<'T> -> 'T

    [<Test>]
    member __.``CloudRef - simple`` () = 
        let ref = run <| CloudRef.New 42
        ref.Value |> runLocal |> should equal 42

    [<Test>]
    member __.``CloudRef - Parallel`` () =
        cloud {
            let! ref = CloudRef.New [1 .. 100]
            let! (x, y) = cloud { let! v = ref.Value in return v.Length } <||> cloud { let! v = ref.Value in return v.Length }
            return x + y
        } |> run |> should equal 200

    [<Test>]
    member __.``CloudRef - Distributed tree`` () =
        let tree = createTree 5 |> run
        getBranchCount tree |> run |> should equal 31


    [<Test>]
    member __.``CloudSequence - simple`` () = 
        let b = run <| CloudSequence.New [1..10000]
        b.Cache() |> runLocal
        b.Count |> runLocal |> should equal 10000
        b.ToEnumerable() |> runLocal |> Seq.sum |> should equal (List.sum [1..10000])

    [<Test>]
    member __.``CloudSequence - parallel`` () =
        let ref = run <| CloudSequence.New [1..10000]
        ref.ToEnumerable() |> runLocal |> Seq.length |> should equal 10000
        cloud {
            let! ref = CloudSequence.New [1 .. 10000]
            let! (x, y) = 
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 
                    <||>
                cloud { let! seq = ref.ToEnumerable() in return Seq.length seq } 

            return x + y
        } |> run |> should equal 20000

    [<Test>]
    member __.``CloudSequence - partitioned`` () =
        cloud {
            let! seqs = CloudSequence.NewPartitioned([|1L .. 1000000L|], 1024L * 1024L)
            seqs.Length |> should be (greaterThanOrEqualTo 8)
            seqs.Length |> should be (lessThan 10)
            let! partialSums = seqs |> Array.map (fun c -> cloud { let! e = c.ToEnumerable() in return Seq.sum e }) |> Cloud.Parallel
            return Array.sum partialSums
        } |> run |> should equal (Array.sum [|1L .. 1000000L|])

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
            let! ch = Cloud.StartChild(cloud { return seq |> Seq.length })
            return! ch
        } |> run |> should equal 100

    [<Test>]
    member __.``CloudFile - simple`` () =
        let file = CloudFile.WriteAllBytes [|1uy .. 100uy|] |> run
        file.GetSizeAsync() |> Async.RunSynchronously |> should equal 100
        cloud {
            let! bytes = CloudFile.ReadAllBytes file
            return bytes.Length
        } |> run |> should equal 100

    [<Test>]
    member __.``CloudFile - large`` () =
        let file =
            cloud {
                let text = Seq.init 1000 (fun _ -> "lorem ipsum dolor sit amet")
                return! CloudFile.WriteLines(text)
            } |> run

        cloud {
            let! lines = CloudFile.ReadLines file
            return Seq.length lines
        } |> run |> should equal 1000

    [<Test>]
    member __.``CloudFile - read from stream`` () =
        let mk a = Array.init (a * 1024) byte
        let n = 512
        cloud {
            let! f = 
                CloudFile.New(fun stream -> async {
                    let b = mk n
                    stream.Write(b, 0, b.Length)
                    stream.Flush()
                    stream.Dispose() })

            let! bytes = CloudFile.ReadAllBytes(f)
            return bytes
        } |> run |> should equal (mk n)

    [<Test>]
    member __.``CloudFile - get by name`` () =
        cloud {
            let! f = CloudFile.WriteAllBytes([|1uy..100uy|])
            let! t = Cloud.StartChild(cloud { 
                let! f' = CloudFile.FromPath f.Path
                return! CloudFile.ReadAllBytes f'   
            })

            return! t
        } |> run |> should equal [|1uy .. 100uy|]

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
            let! container = FileStore.CreateUniqueDirectoryPath()
            let! fileNames = FileStore.Combine(container, Seq.map (sprintf "file%d") [1..10])
            let! files =
                fileNames
                |> Seq.map (fun f -> CloudFile.WriteAllBytes([|1uy .. 100uy|], f))
                |> Cloud.Parallel

            let! files' = CloudFile.Enumerate container
            return files.Length = files'.Length
        } |> run |> should equal true

    [<Test>]
    member __.``CloudFile - attempt to write on stream`` () =
        cloud {
            let! cf = CloudFile.New(fun stream -> async { stream.WriteByte(10uy) })
            return! CloudFile.Read(cf, fun stream -> async { stream.WriteByte(20uy) })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``CloudFile - attempt to read nonexistent file`` () =
        cloud {
            let! cf = CloudFile.FromPath(Guid.NewGuid().ToString())
            return! CloudFile.Read(cf, fun s -> async { return s.ReadByte() })
        } |> runProtected |> Choice.shouldFailwith<_,exn>

    [<Test>]
    member __.``CloudAtom - Sequential updates`` () =
        // avoid capturing test fixture class in closure
        let nseq = nseq
        cloud {
            let! a = CloudAtom.New 0
            for i in 1 .. 10 * nseq do
                do! CloudAtom.Update (fun i -> i + 1) a

            return a
        } |> run |> fun a -> a.Value |> should equal (10 * nseq)

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - Parallel updates`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        let nseq = nseq
        cloud {
            let! a = CloudAtom.New 0
            let worker _ = cloud {
                for _ in 1 .. nseq do
                    do! CloudAtom.Update (fun i -> i + 1) a
            }
            do! Seq.init npar worker |> Cloud.Parallel |> Cloud.Ignore
            return a
        } |> run |> fun a -> a.Value |> should equal (npar * nseq)

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
                return List.sum atom.Value = List.sum [1..npar]
        } |> run |> should equal true

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - transact with contention`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        cloud {
            let! a = CloudAtom.New 0
            let! results = Seq.init npar (fun _ -> CloudAtom.Transact(fun i -> i, (i+1)) a) |> Cloud.Parallel
            return Array.sum results
        } |> run |> should equal (Array.sum [|0 .. npar - 1|])

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - force with contention`` () =
        // avoid capturing test fixture class in closure
        let npar = npar
        cloud {
            let! a = CloudAtom.New -1
            do! Seq.init npar (fun i -> CloudAtom.Force i a) |> Cloud.Parallel |> Cloud.Ignore
            return a.Value
        } |> run |> should be (greaterThanOrEqualTo 0)

    [<Test; Repeat(repeats)>]
    member __.``CloudAtom - dispose`` () =
        cloud {
            let! a = CloudAtom.New 0
            do! cloud { use a = a in () }
            return! CloudAtom.Read a
        } |> runProtected |> Choice.shouldFailwith<_,exn>


[<TestFixture; AbstractClass>]
type ``Local MBrace store tests`` (fileStore, atomProvider, channelProvider, serializer : ISerializer, cache, ?npar, ?nseq) =
    inherit ``MBrace store tests``(?npar = npar, ?nseq = nseq)

    let fileStoreConfig = 
        { 
            FileStore = fileStore
            DefaultDirectory = fileStore.CreateUniqueDirectoryPath ()
            Cache = cache
            Serializer = serializer
        }

    let atomProviderConfig = { AtomProvider = atomProvider ; DefaultContainer = atomProvider.CreateUniqueContainerName() }
    let channelProvider = { ChannelProvider = channelProvider ; DefaultContainer = channelProvider.CreateUniqueContainerName() }

    let resources = resource { 
        yield! InMemory.CreateResources()
        yield fileStoreConfig
        yield atomProviderConfig
        yield channelProvider
    }

    override __.Run(wf : Cloud<'T>, ?ct) = Cloud.RunSynchronously(wf, resources = resources, ?cancellationToken = ct)
    override __.RunLocal(wf : Cloud<'T>) = Cloud.RunSynchronously(wf, resources = resources)