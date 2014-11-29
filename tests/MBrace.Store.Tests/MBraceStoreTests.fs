namespace Nessos.MBrace.Store.Tests

open System
open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.Continuation
open Nessos.MBrace.Tests
open Nessos.MBrace.Store
open Nessos.MBrace.Store.Tests.TestTypes

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``MBrace store tests`` () as self =

    let run wf = self.Run wf 
    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    abstract Run : Cloud<'T> * ?ct:CancellationToken -> 'T

    [<Test>]
    member __.``Simple CloudRef`` () = 
        let ref = run <| CloudRef.New 42
        ref.Value |> should equal 42

    [<Test>]
    member __.``Parallel CloudRef`` () =
        cloud {
            let! ref = CloudRef.New [1 .. 100]
            let! (x, y) = cloud { return ref.Value.Length } <||> cloud { return ref.Value.Length }
            return x + y
        } |> run |> should equal 200

    [<Test>]
    member __.``Distributed tree`` () =
        let tree = createTree 5 |> run
        getBranchCount tree |> run |> should equal 31


    [<Test>]
    member __.``Simple CloudSeq`` () =
        let cseq = CloudSeq.New [|1 .. 10000|] |> run
        cseq |> Seq.length |> should equal 10000
        cseq.Length |> should equal 10000

    [<Test>]
    member __.``Simple CloudFile`` () =
        let file = CloudFile.WriteAllBytes [|1uy .. 100uy|] |> run
        file.GetSizeAsync() |> Async.RunSynchronously |> should equal 100
        cloud {
            let! bytes = CloudFile.ReadAllBytes file
            return bytes.Length
        } |> run |> should equal 100

    [<Test>]
    member __.``Large CloudFile`` () =
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
    member __.``Disposable CloudFile`` () =
        cloud {
            let! file = CloudFile.WriteAllText "lorem ipsum dolor"
            do! cloud { use file = file in () }
            return! CloudFile.ReadAllText file
        } |> runProtected |> Choice.shouldFailwith<_,exn>

//    [<Test>]
//    member __.``Get files in container`` () =
//        cloud {
//            let! container = CloudStore.GetUniqueContainerName()
//            let! file1 = CloudFile.WriteAllBytes([|1uy .. 100uy|], "container")
//            let! file1 = CloudFile.WriteAllBytes [|1uy .. 100uy|]
//        
//        
//        }


[<TestFixture; AbstractClass>]
type ``Local MBrace store tests`` (fileStore, tableStore) =
    inherit ``MBrace store tests``()

    let ctx = StoreConfiguration.mkExecutionContext fileStore tableStore

    override __.Run(wf : Cloud<'T>, ?ct) = Cloud.RunSynchronously(wf, resources = ctx, ?cancellationToken = ct)