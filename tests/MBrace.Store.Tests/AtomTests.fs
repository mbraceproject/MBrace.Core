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
type ``Atom Tests`` (atomProvider : ICloudAtomProvider, ?npar, ?nseq) =

    let testContainer = atomProvider.CreateUniqueContainerName()

    let run x = Async.RunSync x

    let npar = defaultArg npar 20
    let nseq = defaultArg nseq 20

    [<TestFixtureTearDown>]
    member __.TearDown() =
        atomProvider.DisposeContainer testContainer |> run

    [<Test>]
    member __.``UUID is not null or empty.`` () = 
        String.IsNullOrEmpty atomProvider.Id
        |> should equal false

    [<Test>]
    member __.``Store factory should generate identical instances`` () =
        let fact = atomProvider.GetAtomProviderDescriptor() |> FsPickler.Clone
        let atomProvider' = fact.Recover()
        atomProvider'.Name |> should equal atomProvider.Name
        atomProvider'.Id |> should equal atomProvider.Id

    [<Test>]
    member __.``Create, dereference and delete`` () =
        let value = ("key",42)
        let atom = atomProvider.CreateAtom(testContainer, value) |> run
        atom.Value |> should equal value
        atom.GetValue() |> run |> should equal value
        (atom :> ICloudDisposable).Dispose() |> run
        shouldfail (fun () -> atom.Value |> ignore)

    [<Test>]
    member __.``Create and dispose container`` () =
        let container = atomProvider.CreateUniqueContainerName()
        let atom = atomProvider.CreateAtom(container, 42) |> run
        atom.Value |> should equal 42
        atomProvider.DisposeContainer container |> run
        shouldfail (fun () -> atom.Value |> ignore)

    [<Test>]
    member __.``Update sequentially`` () =
        let atom = atomProvider.CreateAtom(testContainer,0) |> run
        for i = 1 to 10 * nseq do 
            atom.Update(fun i -> i + 1) |> run

        atom.Value |> should equal (10 * nseq)

    [<Test; Repeat(repeats)>]
    member __.``Update with contention -- int`` () =
        let atom = atomProvider.CreateAtom(testContainer, 0) |> run
        let worker _ = async {
            for i in 1 .. nseq do
                do! atom.Update(fun i -> i + 1)
        }

        Array.init npar worker |> Async.Parallel |> Async.Ignore |> run
        atom.Value |> should equal (npar * nseq)

    [<Test; Repeat(repeats)>]
    member __.``Update with contention -- list`` () =
        if atomProvider.IsSupportedValue [1..100] then
            let atom = atomProvider.CreateAtom<int list>(testContainer, []) |> run
            let worker _ = async {
                for i in 1 .. nseq do
                    do! atom.Update(fun xs -> i :: xs)
            }

            Array.init npar worker |> Async.Parallel |> Async.Ignore |> run
            atom.Value |> List.length |> should equal (npar * nseq)

    [<Test; Repeat(repeats)>]
    member __.``Force value`` () =
        let npar = npar
        if atomProvider.IsSupportedValue [1..100] then
            let atom = atomProvider.CreateAtom<int>(testContainer, 0) |> run

            let worker i = async {
                if i = npar / 2 then
                    do! atom.Force 42
                else
                    do! atom.Update id
            }

            Array.init npar worker |> Async.Parallel |> Async.Ignore |> run
            atom.Value |> should equal 42