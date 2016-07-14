namespace MBrace.Core.Tests

open System

open NUnit.Framework
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Core.Internals

[<TestFixture; AbstractClass>]
type ``CloudAtom Tests`` (parallelismFactor : int) as self =

    static let nSequential = 100

    let runOnCloud wf = self.Run wf 

    let repeat f = repeat self.Repeats f

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
        let nSequential = nSequential
        let comp = cloud {
            use! atom = CloudAtom.New 0
            let updater _ = local {
                for _ in 1 .. nSequential do
                    atom.Update((+) 1)
            }

            let! _ = Seq.init parallelismFactor updater |> Cloud.Parallel

            return atom.Value
        } 
        
        test <@ runOnCloud comp = parallelismFactor * nSequential @>

    [<Test>]
    member __.``Sequential updates`` () =
        let nSequential = nSequential
        // avoid capturing test fixture class in closures
        let atom =
            local {
                let! a = CloudAtom.New 0
                for _ in 1 .. nSequential do
                    do! CloudAtom.Increment a |> Local.Ignore

                return a
            } |> runOnCloud
            
        try test <@ atom.Value = nSequential @>
        finally atom.Dispose() |> Async.RunSync

    [<Test>]
    member __.``Parallel updates`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            let nSequential = nSequential
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
        
            try test <@ atom.Value = parallelismFactor * nSequential @>
            finally atom.Dispose() |> Async.RunSync)

    [<Test>]
    member __.``Parallel updates with large obj`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                let! isSupported = CloudAtom.IsSupportedValue [1 .. parallelismFactor]
                if isSupported then
                    use! atom = CloudAtom.New List.empty<int>
                    do! Seq.init parallelismFactor (fun i -> cloud { return! atom.UpdateAsync(fun is -> i + 1 :: is) }) |> Cloud.Parallel |> Cloud.Ignore
                    let! values = atom.GetValueAsync()
                    return test <@ set values = set [1 .. parallelismFactor] @>
            } |> runOnCloud)

    [<Test>]
    member __.``Transact with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                use! a = CloudAtom.New 0
                let! results = Seq.init parallelismFactor (fun _ -> cloud { return! a.TransactAsync(fun i -> i, i+1) }) |> Cloud.Parallel
                return test <@ Array.sum results = Array.sum [|0 .. parallelismFactor - 1|] @>
            } |> runOnCloud)

    [<Test>]
    member __.``Force with contention`` () =
        repeat(fun () ->
            // avoid capturing test fixture class in closures
            let parallelismFactor = parallelismFactor
            cloud {
                use! a = CloudAtom.New 0
                do! Seq.init parallelismFactor (fun i -> cloud { return! a.ForceAsync(i + 1) }) |> Cloud.Parallel |> Cloud.Ignore
                let! result = a.GetValueAsync()
                test <@ result > 0 @>
            } |> runOnCloud)

    [<Test>]
    member __.``Named lookups`` () =
        if __.IsSupportedNamedLookup then
            let comp = cloud {
                use! atom = CloudAtom.New(41)
                let! atom' = CloudAtom.GetById<int>(atom.Id)
                return! CloudAtom.Increment atom'
            } 

            test <@ runOnCloud comp = 42 @>

    [<Test>]
    member __.``Dispose`` () =
        repeat(fun () ->
            let comp = cloud {
                let! a = CloudAtom.New 0
                do! cloud { use _a = a in () }
                return! a.GetValueAsync()
            }

            raises <@ runOnCloud comp @>)