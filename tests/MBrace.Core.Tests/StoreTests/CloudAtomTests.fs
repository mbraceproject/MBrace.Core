namespace MBrace.Core.Tests

open System

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals

[<TestFixture; AbstractClass>]
type ``CloudAtom Tests`` (parallelismFactor : int) as self =

    static let nSequential = 100

    let runOnCloud wf = self.Run wf 
    let runOnCurrentProcess wf = self.RunLocally wf

    let repeat f = repeat self.Repeats f

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    /// Maximum number of repeats to run nondeterministic tests
    abstract Repeats : int
    /// Determines whether current CloudAtom implementation supports named lookups
    abstract IsSupportedNamedLookup : bool

    [<Test>]
    member __.``Update with contention`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            use! atom = CloudAtom.New 0
            let updater _ = local {
                for i in 1 .. nSequential do
                    do! CloudAtom.Update (atom, (+) 1)
            }

            let! _ = Seq.init parallelismFactor updater |> Cloud.Parallel

            return! CloudAtom.Read atom
        } |> runOnCloud |> shouldEqual (parallelismFactor * nSequential)

    [<Test>]
    member __.``Sequential updates`` () =
        // avoid capturing test fixture class in closures
        let atom =
            local {
                let! a = CloudAtom.New 0
                for i in 1 .. nSequential do
                    do! CloudAtom.Increment a |> Local.Ignore

                return a
            } |> runOnCloud
            
        try atom.Value |> shouldEqual nSequential
        finally atom.Dispose() |> Async.RunSync

    [<Test>]
    member __.``Parallel updates`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            let atom = 
                cloud {
                    let! a = CloudAtom.New 0
                    let worker _ = local {
                        for _ in 1 .. nSequential do
                            do! CloudAtom.Increment a |> Local.Ignore
                    }
                    do! Seq.init parallelismFactor worker |> Cloud.Parallel |> Cloud.Ignore
                    return a
                } |> runOnCloud
        
            try atom.Value |> shouldEqual (parallelismFactor * nSequential)
            finally atom.Dispose() |> Async.RunSync)

    [<Test>]
    member __.``Parallel updates with large obj`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                let! isSupported = CloudAtom.IsSupportedValue [1 .. parallelismFactor]
                if isSupported then return true
                else
                    use! atom = CloudAtom.New List.empty<int>
                    do! Seq.init parallelismFactor (fun i -> CloudAtom.Update (atom, fun is -> i :: is)) |> Cloud.Parallel |> Cloud.Ignore
                    let! values = CloudAtom.Read atom
                    return List.sum values = List.sum [1 .. parallelismFactor]
            } |> runOnCloud |> shouldEqual true)

    [<Test>]
    member __.``Transact with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                use! a = CloudAtom.New 0
                let! results = Seq.init parallelismFactor (fun _ -> CloudAtom.Transact(a, fun i -> i, i+1)) |> Cloud.Parallel
                return Array.sum results
            } |> runOnCloud |> shouldEqual (Array.sum [|0 .. parallelismFactor - 1|]))

    [<Test>]
    member __.``Force with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                use! a = CloudAtom.New 0
                do! Seq.init parallelismFactor (fun i -> CloudAtom.Force(a, i + 1)) |> Cloud.Parallel |> Cloud.Ignore
                return! CloudAtom.Read a
            } |> runOnCloud |> shouldBe (fun i -> i > 0))

    [<Test>]
    member __.``Named lookups`` () =
        if __.IsSupportedNamedLookup then
            cloud {
                use! atom = CloudAtom.New(41)
                let! atom' = CloudAtom.GetById<int>(atom.Id)
                return! CloudAtom.Increment atom'
            } |> runOnCloud |> shouldEqual 42

    [<Test>]
    member __.``Dispose`` () =
        repeat(fun () ->
            cloud {
                let! a = CloudAtom.New 0
                do! cloud { use a = a in () }
                return! CloudAtom.Read a
            } |> runProtected |> Choice.shouldFailwith<_,exn>)