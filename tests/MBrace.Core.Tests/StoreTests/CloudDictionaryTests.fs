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
    let runLocally wf = self.RunLocally wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Specifies if test is running in-memory
    abstract IsInMemoryFixture : bool
    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
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
                do! dict.Add(string i, i) |> Async.Ignore

            let! values = dict.ToEnumerable()
            return values |> Seq.map (fun kv -> kv.Value) |> Seq.sum
        } |> runRemote |> shouldEqual 5050

    [<Test>]
    member __.``concurrent adds`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            let! dict = CloudDictionary.New<int> ()
            let add i = local { return! dict.Add(string i, i) }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> add i ] |> Cloud.Ignore

            return! dict.Count
        } |> runRemote |> shouldEqual (int64 parallelismFactor)

    [<Test>]
    member __.``concurrent add or update`` () =
        // skip test when running in Travis and In-Memory due to ConcurrentDictionary bug
        // https://bugzilla.xamarin.com/show_bug.cgi?id=29047
        if runsOnMono && isTravisInstance && __.IsInMemoryFixture then () else
            let parallelismFactor = parallelismFactor
            cloud {
                let! dict = CloudDictionary.New<int> ()
                let incr i = local {
                    let! _ = dict.AddOrUpdate("key", function None -> i | Some c -> c + i)
                    return ()
                }

                do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> incr i ] |> Cloud.Ignore
                return! dict.TryFind "key"
            } |> runRemote |> shouldEqual (Some (Array.sum [|1 .. parallelismFactor|]))
