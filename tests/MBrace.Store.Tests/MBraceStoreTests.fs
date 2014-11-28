namespace Nessos.MBrace.Store.Tests

open System
open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Tests

open Nessos.FsPickler

open NUnit.Framework
open FsUnit

[<TestFixture; AbstractClass>]
type ``MBrace store tests`` () as self =

    let run wf = self.Run wf 

    abstract Run : Cloud<'T> -> Choice<'T, exn>

    [<Test>]
    member __.``Simple CloudRef`` () = 
        run(CloudRef.New 42) |> Choice.shouldMatch (fun r -> r.Value = 42)


[<TestFixture; AbstractClass>]
type ``Local MBrace store tests`` (fileStore, tableStore) =
    inherit ``MBrace store tests``()

    let ctx = StoreConfiguration.mkExecutionContext fileStore tableStore

    override __.Run(wf : Cloud<'T>) = Cloud.RunProtected(wf, resources = ctx)