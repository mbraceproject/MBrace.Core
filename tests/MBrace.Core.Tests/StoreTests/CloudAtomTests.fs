namespace MBrace.Tests

open System

open NUnit.Framework

open MBrace
open MBrace.Store

[<TestFixture; AbstractClass>]
type ``CloudAtom Tests`` (nParallel : int) as self =

    static let nSequential = 100

    let runRemote wf = self.Run wf 
    let runLocal wf = self.RunLocal wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocal : Cloud<'T> -> 'T
    /// Local store client instance
    abstract StoreClient : StoreClient


    [<Test>]
    member __.``Local StoreClient`` () =
        let sc = __.StoreClient
        let atom = sc.CloudAtom.New(41) |> Async.RunSynchronously
        sc.CloudAtom.Update((+) 1) atom |> Async.RunSynchronously
        sc.CloudAtom.Read atom
        |> Async.RunSynchronously
        |> shouldEqual 42

    [<Test>]
    member __.``Atom: update with contention`` () =
        let nParallel = nParallel
        cloud {
            let! atom = CloudAtom.New 0
            let updater _ = cloud {
                for i in 1 .. nSequential do
                    do! CloudAtom.Update ((+) 1) atom
            }

            let! _ = Seq.init nParallel updater |> Cloud.Parallel

            return! CloudAtom.Read atom
        } |> runRemote |> shouldEqual (nParallel * nSequential)

    [<Test>]
    member __.``CloudAtom - Sequential updates`` () =
        // avoid capturing test fixture class in closures
        let atom =
            cloud {
                let! a = CloudAtom.New 0
                for i in 1 .. nSequential do
                    do! CloudAtom.Incr a

                return a
            } |> runRemote
            
        atom.Value |> runLocal |> shouldEqual nSequential

    [<Test; Repeat(Config.repeats)>]
    member __.``CloudAtom - Parallel updates`` () =
        // avoid capturing test fixture class in closures
        let nParallel = nParallel
        let atom = 
            cloud {
                let! a = CloudAtom.New 0
                let worker _ = cloud {
                    for _ in 1 .. nSequential do
                        do! CloudAtom.Incr a
                }
                do! Seq.init nParallel worker |> Cloud.Parallel |> Cloud.Ignore
                return a
            } |> runRemote
        
        atom.Value |> runLocal |> shouldEqual (nParallel * nSequential)

    [<Test; Repeat(Config.repeats)>]
    member __.``CloudAtom - Parallel updates with large obj`` () =
        // avoid capturing test fixture class in closures
        let nParallel = nParallel
        cloud {
            let! isSupported = CloudAtom.IsSupportedValue [1 .. nParallel]
            if isSupported then return true
            else
                let! atom = CloudAtom.New List.empty<int>
                do! Seq.init nParallel (fun i -> CloudAtom.Update (fun is -> i :: is) atom) |> Cloud.Parallel |> Cloud.Ignore
                let! values = atom.Value
                return List.sum values = List.sum [1 .. nParallel]
        } |> runRemote |> shouldEqual true

    [<Test; Repeat(Config.repeats)>]
    member __.``CloudAtom - transact with contention`` () =
        // avoid capturing test fixture class in closures
        let nParallel = nParallel
        cloud {
            let! a = CloudAtom.New 0
            let! results = Seq.init nParallel (fun _ -> CloudAtom.Transact(fun i -> i, (i+1)) a) |> Cloud.Parallel
            return Array.sum results
        } |> runRemote |> shouldEqual (Array.sum [|0 .. nParallel - 1|])

    [<Test; Repeat(Config.repeats)>]
    member __.``CloudAtom - force with contention`` () =
        // avoid capturing test fixture class in closures
        let nParallel = nParallel
        cloud {
            let! a = CloudAtom.New -1
            do! Seq.init nParallel (fun i -> CloudAtom.Force i a) |> Cloud.Parallel |> Cloud.Ignore
            return! a.Value
        } |> runRemote |> shouldBe (fun i -> i > 0)

    [<Test; Repeat(Config.repeats)>]
    member __.``CloudAtom - dispose`` () =
        cloud {
            let! a = CloudAtom.New 0
            do! cloud { use a = a in () }
            return! CloudAtom.Read a
        } |> runProtected |> Choice.shouldFailwith<_,exn>