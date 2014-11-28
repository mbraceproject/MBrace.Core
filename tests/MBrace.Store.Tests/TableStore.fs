namespace Nessos.MBrace.Store.Tests

open System

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<AutoOpen>]
module private Helpers =
    [<Literal>]
#if DEBUG
    let repeats = 10
#else
    let repeats = 3
#endif

[<TestFixture; AbstractClass>]
type ``Table Store Tests`` (tableStore : ICloudTableStore) =
    do StoreRegistry.Register(tableStore, force = true)

    let run x = Async.RunSync x

    [<Test>]
    member __.``UUID is not null or empty.`` () = 
        String.IsNullOrEmpty tableStore.UUID
        |> should equal false

    [<Test>]
    member __.``Store factory should generate identical instances`` () =
        let fact = tableStore.GetFactory() |> FsPickler.Clone
        let tableStore' = fact.Create()
        tableStore'.UUID |> should equal tableStore.UUID

    [<Test>]
    member __.``Create, dereference and delete`` () =
        let value = ("key",42)
        let id = tableStore.Create value |> run
        tableStore.Exists id |> run |> should equal true
        tableStore.GetValue<string * int> id |> run |> should equal value
        tableStore.Delete id |> run
        tableStore.Exists id |> run |> should equal false

    [<Test>]
    member __.``Create and enumerate`` () =
        let id = tableStore.Create 42 |> run
        tableStore.EnumerateKeys() |> run |> Array.exists ((=) id) |> should equal true
        tableStore.Delete id |> run

    [<Test>]
    member __.``Update sequentially`` () =
        let id = tableStore.Create 0 |> run
        for i = 1 to 100 do 
            tableStore.Update(id, fun i -> i + 1) |> run

        tableStore.GetValue<int>(id) |> run |> should equal 100
        tableStore.Delete id |> run

    [<Test; Repeat(repeats)>]
    member __.``Update with contention -- int`` () =
        let id = tableStore.Create 0 |> run
        let worker _ = async {
            for i in 1 .. 20 do
                do! tableStore.Update(id, fun i -> i + 1)
        }

        Array.init 20 worker |> Async.Parallel |> Async.Ignore |> run
        tableStore.GetValue<int>(id) |> run |> should equal 400
        tableStore.Delete id |> run

    [<Test; Repeat(repeats)>]
    member __.``Update with contention -- list`` () =
        if tableStore.IsSupportedValue [1..100] then
            let id = tableStore.Create<int list> [] |> run
            let worker _ = async {
                for i in 1 .. 10 do
                    do! tableStore.Update(id, fun xs -> i :: xs)
            }

            Array.init 10 worker |> Async.Parallel |> Async.Ignore |> run
            tableStore.GetValue<int list>(id) |> run |> List.length |> should equal 100
            tableStore.Delete id |> run

    [<Test; Repeat(repeats)>]
    member __.``Force value`` () =
        if tableStore.IsSupportedValue [1..100] then
            let id = tableStore.Create<int> 0 |> run

            let worker i = async {
                if i = 5 then
                    do! tableStore.Force(id, 42)
                else
                    do! tableStore.Update<int>(id, fun i -> i)
            }

            Array.init 10 worker |> Async.Parallel |> Async.Ignore |> run
            tableStore.GetValue<int>(id) |> run |> should equal 42
            tableStore.Delete id |> run