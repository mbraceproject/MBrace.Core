namespace MBrace.Core.Tests

open System

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals

[<TestFixture; AbstractClass>]
type ``CloudAtom Tests`` (parallelismFactor : int) as self =

    static let nSequential = 100

    let runOnCloud wf = self.RunOnCloud wf 
    let runOnClient wf = self.RunOnClient wf

    let repeat f = repeat self.Repeats f

    let runProtected wf = 
        try self.RunOnCloud wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract RunOnCloud : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunOnClient : Cloud<'T> -> 'T
    /// Maximum number of repeats to run nondeterministic tests
    abstract Repeats : int

    [<Test>]
    member __.``Atom: update with contention`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! atom = CloudAtom.New 0
            let updater _ = cloud {
                for i in [1 .. nSequential] do
                    do! CloudAtom.Update (atom, (+) 1)
            }

            let! _ = Seq.init parallelismFactor updater |> Cloud.Parallel

            return! CloudAtom.Read atom
        } |> runOnCloud |> shouldEqual (parallelismFactor * nSequential)

    [<Test>]
    member __.``CloudAtom - Sequential updates`` () =
        // avoid capturing test fixture class in closures
        let atom =
            cloud {
                let! a = CloudAtom.New 0
                for i in [1 .. nSequential] do
                    do! CloudAtom.Incr a |> Local.Ignore

                return a
            } |> runOnCloud
            
        atom.Value |> shouldEqual nSequential

    [<Test>]
    member __.``CloudAtom - Parallel updates`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            let atom = 
                cloud {
                    let! a = CloudAtom.New 0
                    let worker _ = cloud {
                        for _ in [1 .. nSequential] do
                            do! CloudAtom.Incr a |> Local.Ignore
                    }
                    do! Seq.init parallelismFactor worker |> Cloud.Parallel |> Cloud.Ignore
                    return a
                } |> runOnCloud
        
            atom.Value |> shouldEqual (parallelismFactor * nSequential))

    [<Test>]
    member __.``CloudAtom - Parallel updates with large obj`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                let! isSupported = CloudAtom.IsSupportedValue [1 .. parallelismFactor]
                if isSupported then return true
                else
                    let! atom = CloudAtom.New List.empty<int>
                    do! Seq.init parallelismFactor (fun i -> CloudAtom.Update (atom, fun is -> i :: is)) |> Cloud.Parallel |> Cloud.Ignore
                    let! values = CloudAtom.Read atom
                    return List.sum values = List.sum [1 .. parallelismFactor]
            } |> runOnCloud |> shouldEqual true)

    [<Test>]
    member __.``CloudAtom - transact with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                let! a = CloudAtom.New 0
                let! results = Seq.init parallelismFactor (fun _ -> CloudAtom.Transact(a, fun i -> i, i+1)) |> Cloud.Parallel
                return Array.sum results
            } |> runOnCloud |> shouldEqual (Array.sum [|0 .. parallelismFactor - 1|]))

    [<Test>]
    member __.``CloudAtom - force with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                let! a = CloudAtom.New 0
                do! Seq.init parallelismFactor (fun i -> CloudAtom.Force(a, i + 1)) |> Cloud.Parallel |> Cloud.Ignore
                return! CloudAtom.Read a
            } |> runOnCloud |> shouldBe (fun i -> i > 0))

    [<Test>]
    member __.``CloudAtom - dispose`` () =
        repeat(fun () ->
            cloud {
                let! a = CloudAtom.New 0
                do! cloud { use a = a in () }
                return! CloudAtom.Read a
            } |> runProtected |> Choice.shouldFailwith<_,exn>)