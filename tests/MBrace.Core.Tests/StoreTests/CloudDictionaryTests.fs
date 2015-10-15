namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Library

open NUnit.Framework

[<TestFixture; AbstractClass>]
type ``CloudDictionary Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf 
    let runOnCurrentProcess wf = self.RunLocally wf

    let runProtected wf = 
        try self.Run wf |> Choice1Of2
        with e -> Choice2Of2 e

    /// Specifies if test is running in-memory
    abstract IsInMemoryFixture : bool
    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    /// Specifies whether current implementation support named lookups
    abstract IsSupportedNamedLookup : bool

    [<Test>]
    member __.``Add/remove`` () =
        cloud {
            use! dict = CloudDictionary.New<int> ()
            let! _ = dict.AddAsync("key", 42)
            let! contains = dict.ContainsKeyAsync "key"
            contains |> shouldEqual true
            return! dict.TryFindAsync "key"
        } |> runOnCloud |> shouldEqual (Some 42)

    [<Test>]
    member __.``Multiple adds`` () =
        cloud {
            use! dict = CloudDictionary.New<int> ()
            for i in [1 .. 100] do
                do! dict.AddAsync(string i, i)

            let! values = Cloud.OfAsync <| dict.GetEnumerableAsync()
            return values |> Seq.map (fun kv -> kv.Value) |> Seq.sum
        } |> runOnCloud |> shouldEqual 5050

    [<Test>]
    member __.``Concurrent adds`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            use! dict = CloudDictionary.New<int> ()
            let add i = local { return! dict.AddAsync(string i, i) }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> add i ] |> Cloud.Ignore

            return! dict.GetCountAsync() |> Cloud.OfAsync
        } |> runOnCloud |> shouldEqual (int64 parallelismFactor)

    [<Test>]
    member __.``Concurrent add or update`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            use! dict = CloudDictionary.New<int> ()
            let incr i = local {
                let! _ = dict.AddOrUpdateAsync ("key", function None -> i | Some c -> c + i)
                return ()
            }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> incr i ] |> Cloud.Ignore
            return! dict.TryFindAsync "key"
        } |> runOnCloud |> shouldEqual (Some (Array.sum [|1 .. parallelismFactor|]))

    [<Test>]
    member __.``Named lookup`` () =
        if __.IsSupportedNamedLookup then
            cloud {
                use! dict = CloudDictionary.New<int> ()
                do! dict.AddAsync("testKey", 42)
                let! dict' = CloudDictionary.GetById<int>(dict.Id)
                return! dict'.TryFindAsync "testKey"
            } |> runOnCloud |> shouldEqual (Some 42)
