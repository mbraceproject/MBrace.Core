namespace MBrace.Core.Tests

open System

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Library

open NUnit.Framework
open Swensen.Unquote.Assertions

[<TestFixture; AbstractClass>]
type ``CloudDictionary Tests`` (parallelismFactor : int) as self =

    let runOnCloud wf = self.Run wf

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
            let! _ = dict.ForceAddAsync("key", 42)
            let! contains = dict.ContainsKeyAsync "key"
            test <@ contains = true @>
            let! result = dict.TryFindAsync "key"
            test <@ result = Some 42 @>
        } |> runOnCloud

    [<Test>]
    member __.``Multiple adds`` () =
        cloud {
            use! dict = CloudDictionary.New<int> ()
            for i in [1 .. 100] do
                do! dict.ForceAddAsync(string i, i)

            let! values = Cloud.OfAsync <| dict.GetEnumerableAsync()
            let sum = values |> Seq.map (fun kv -> kv.Value) |> Seq.sum
            test <@ sum = 5050 @>
        } |> runOnCloud

    [<Test>]
    member __.``Concurrent adds`` () =
        let parallelismFactor = parallelismFactor
        cloud {
            use! dict = CloudDictionary.New<int> ()
            let add i = local { return! dict.ForceAddAsync(string i, i) }

            do! Cloud.Parallel [ for i in 1 .. parallelismFactor -> add i ] |> Cloud.Ignore

            let! count = dict.GetCountAsync()
            test <@ count = int64 parallelismFactor @>
        } |> runOnCloud

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
            let! value = dict.TryFindAsync "key"
            test <@ value = (Some (Array.sum [|1 .. parallelismFactor|])) @>
        } |> runOnCloud

    [<Test>]
    member __.``Named lookup`` () =
        if __.IsSupportedNamedLookup then
            cloud {
                use! dict = CloudDictionary.New<int> ()
                do! dict.ForceAddAsync("testKey", 42)
                let! dict' = CloudDictionary.GetById<int>(dict.Id)
                let! value = dict'.TryFindAsync "testKey"
                test <@ value = Some 42 @>
            } |> runOnCloud