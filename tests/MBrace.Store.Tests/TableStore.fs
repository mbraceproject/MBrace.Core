namespace Nessos.MBrace.Store.Tests

open System

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``Table Store Tests`` (tableStore : ICloudTableStore) =
    do StoreRegistry.Register(tableStore, force = true)

    let run x = Async.RunSync x

    [<Literal>]
#if DEBUG
    let repeats = 10
#else
    let repeats = 3
#endif

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
    member __.``Create and delete table entry`` () =
        let id = tableStore.Create 42 |> run
        tableStore.Exists id |> run |> should equal true
        tableStore.GetValue<int> id |> run |> should equal 42
        tableStore.Delete id |> run
        tableStore.Exists id |> run |> should equal false