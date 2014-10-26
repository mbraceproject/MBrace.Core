namespace Nessos.MBrace.Tests

open System.Threading

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.Runtime

type Cloud =
    static member RunProtected(comp, ?resources) =
        try Cloud.RunSynchronously(comp, ?resources = resources, ?cancellationToken = None) |> Choice1Of2
        with e -> Choice2Of2 e

    static member RunProtected(comp : CancellationTokenSource -> Cloud<'T>, ?resources) =
        let cts = new System.Threading.CancellationTokenSource()
        let comp = comp cts
        try Cloud.RunSynchronously(comp, ?resources = resources, cancellationToken = cts.Token) |> Choice1Of2
        with e -> Choice2Of2 e

module Choice =

    let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 v' -> should equal value v'
        | Choice2Of2 e -> should equal value e

    let shouldMatch (pred : 'T -> bool) (input : Choice<'T, exn>) =
        match input with
        | Choice1Of2 t when pred t -> ()
        | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
        | Choice2Of2 e -> should be instanceOfType<'T> e

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 t -> should be instanceOfType<'Exn> t
        | Choice2Of2 e -> should be instanceOfType<'Exn> e

[<RequireQualifiedAccess>]
module List =

    /// <summary>
    ///     split list at given length
    /// </summary>
    /// <param name="n">splitting point.</param>
    /// <param name="xs">input list.</param>
    let splitAt n (xs : 'a list) =
        let rec splitter n (left : 'a list) right =
            match n, right with
            | 0 , _ | _ , [] -> List.rev left, right
            | n , h :: right' -> splitter (n-1) (h::left) right'

        splitter n [] xs

    /// <summary>
    ///     split list in half
    /// </summary>
    /// <param name="xs">input list</param>
    let split (xs : 'a list) = splitAt (xs.Length / 2) xs

type DummyDisposable() =
    let isDisposed = ref false
    interface ICloudDisposable with
        member __.Dispose () = async { isDisposed := true }

    member __.IsDisposed = !isDisposed