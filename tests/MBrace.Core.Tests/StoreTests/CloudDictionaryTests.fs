namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Library

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudDictionary Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf 
    let runOnCurrentProcess wf = self.RunOnCurrentProcess wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Specifies if test is running in-memory
    abstract IsInMemoryFixture : bool
    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunOnCurrentProcess : Cloud<'T> -> 'T

    [<Test>]
    member __.``Add/remove`` () =
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let! _ = dict.Add("key", 42)
            let! contains = dict.ContainsKey "key"
            contains |> shouldEqual true
            return! dict.TryFind "key"
        } |> runOnCloud |> shouldEqual (Some 42)

    [<Test>]
    member __.``Multiple adds`` () =
        cloud {
            let! dict = CloudDictionary.New<int> ()
            for i in [1 .. 100] do
                do! dict.Add(string i, i) |> Async.Ignore

            let! values = dict.ToEnumerable()
            return values |> Seq.map (fun kv -> kv.Value) |> Seq.sum
        } |> runOnCloud |> shouldEqual 5050

    [<Test>]
    member __.``Concurrent adds`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let add i = local { return! dict.Add(string i, i) }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> add i ] |> Cloud.Ignore

            return! dict.GetCount()
        } |> runOnCloud |> shouldEqual (int64 parallelismFactor)

    [<Test>]
    member __.``Concurrent add or update`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let incr i = local {
                let! _ = CloudDictionary.AddOrUpdate "key" (function None -> i | Some c -> c + i) dict
                return ()
            }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> incr i ] |> Cloud.Ignore
            return! dict.TryFind "key"
        } |> runOnCloud |> shouldEqual (Some (Array.sum [|1 .. parallelismFactor|]))
