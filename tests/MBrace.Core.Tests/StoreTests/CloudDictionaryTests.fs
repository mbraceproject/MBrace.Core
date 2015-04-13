namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Store
open MBrace.Workflows
open MBrace.Client

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudDictionary Tests`` (parallelismFactor : int) as self =

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
    abstract DictionaryClient : CloudDictionaryClient


    [<Test>]
    member __.``Local StoreClient`` () =
        let dc = __.DictionaryClient
        let dict = dc.New()
        dc.TryAdd "key" 42 dict |> shouldEqual true
        dc.ContainsKey "key" dict |> shouldEqual true
        dc.TryFind "key" dict |> shouldEqual (Some 42)
        dc.Remove "key" dict |> shouldEqual true
        dc.ContainsKey "key" dict |> shouldEqual false

    [<Test>]
    member __.``add/remove`` () =
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let! _ = dict.Add("key", 42)
            let! contains = dict.ContainsKey "key"
            contains |> shouldEqual true
            return! dict.TryFind "key"
        } |> runRemote |> shouldEqual (Some 42)

    [<Test>]
    member __.``multiple adds`` () =
        cloud {
            let! dict = CloudDictionary.New<int> ()
            for i in [1 .. 100] do
                do! dict.Add(string i, i) |> Cloud.Ignore

            let! values = dict.ToEnumerable()
            return values |> Seq.map (fun kv -> kv.Value) |> Seq.sum
        } |> runRemote |> shouldEqual 5050

    [<Test>]
    member __.``concurrent adds`` () =
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let add i = dict.Add(string i, i)

            do! Cloud.Parallel [ for i in 1 .. 100 -> add i ] |> Cloud.Ignore

            return! dict.Count
        } |> runRemote |> shouldEqual 100L

    [<Test>]
    member __.``concurrent add or update`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let incr i = local {
                let! _ = dict.AddOrUpdate("key", function None -> i | Some c -> c + i)
                return ()
            }

            do! Cloud.Parallel [ for i in 1 .. 100 -> incr i ] |> Cloud.Ignore
            return! dict.TryFind "key"
        } |> runRemote |> shouldEqual (Some 5050)
