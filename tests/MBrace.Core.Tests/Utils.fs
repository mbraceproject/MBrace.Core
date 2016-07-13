namespace MBrace.Core.Tests

open FsCheck

open System
open System.Collections.Generic
open System.IO
open System.Threading

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals

[<AutoOpen>]
module Utils =

    let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
    let isAppVeyorInstance = System.Environment.GetEnvironmentVariable("APPVEYOR") <> null
    let isTravisInstance = System.Environment.GetEnvironmentVariable("TRAVIS") <> null
    let isCIInstance = isAppVeyorInstance || isTravisInstance

    /// repeats computation (test) for a given number of times
    let repeat (maxRepeats : int) (f : unit -> unit) : unit =
        for _ in 1 .. maxRepeats do f ()

    type Check private () =
        static let genSeq maxItems minSz maxSz (gen : Gen<'T>) =
            let SizeInterval = float (maxSz - minSz) / float maxItems
            let incr (currSize : int) = float currSize + SizeInterval |> round |> int
            let rec aux i size rnd = seq {
                if i < maxItems then
                    let rnd0, rnd1 = Random.split rnd
                    yield Gen.eval size rnd1 gen
                    yield! aux (i + 1) (incr size) rnd0
            }

            aux 0 minSz (Random.newSeed())

        static let checkNoShrink (config:Config) (f : 'T -> bool) =
            for t in genSeq config.MaxTest config.StartSize config.EndSize Arb.from<'T>.Generator do
                try 
                    if f t then ()
                    else
                        failwithf "Falsified: %A" t

                with e ->
                    failwithf "Input %A resulted in exception: %O" t e

        static let checkNoShrink2 config (f : 'T -> unit) = checkNoShrink config (fun t -> f t; true)

        /// quick check methods with explicit type annotation
        static member QuickThrowOnFail<'T> (f : 'T -> unit, ?maxRuns, ?shrink : bool) = 
            let config = 
                match maxRuns with 
                | None -> Config.QuickThrowOnFailure
                | Some mr -> { Config.QuickThrowOnFailure with MaxTest = mr }

            if defaultArg shrink true then
                Check.One(config, f)
            else
                checkNoShrink2 config f

        /// quick check methods with explicit type annotation
        static member QuickThrowOnFail<'T> (f : 'T -> bool, ?maxRuns, ?shrink) = 
            let config = 
                match maxRuns with 
                | None -> Config.QuickThrowOnFailure
                | Some mr -> { Config.QuickThrowOnFailure with MaxTest = mr }

            if defaultArg shrink true then
                Check.One(config, f)
            else
                checkNoShrink config f

    [<AutoSerializable(false)>]
    type private DisposableEnumerable<'T>(isDisposed : bool ref, ts : seq<'T>) =
        let check() = if !isDisposed then raise <| new System.ObjectDisposedException("enumerator")
        let e = ts.GetEnumerator()
        interface IEnumerator<'T> with
            member __.Current = check () ; e.Current
            member __.Current = check () ; box e.Current
            member __.MoveNext () = check () ; e.MoveNext()
            member __.Dispose () = check () ; isDisposed := true ; e.Dispose()
            member __.Reset () = check () ; e.Reset()
            
    [<AutoSerializable(true)>]
    type internal DisposableSeq<'T> (ts : seq<'T>) =
        let isDisposed = ref false

        member __.IsDisposed = !isDisposed

        interface seq<'T> with
            member __.GetEnumerator() = new DisposableEnumerable<'T>(isDisposed, ts) :> IEnumerator<'T>
            member __.GetEnumerator() = new DisposableEnumerable<'T>(isDisposed, ts) :> System.Collections.IEnumerator

    let internal dseq ts = new DisposableSeq<'T>(ts)

    type internal InMemoryCancellationToken(token : CancellationToken) =
        new () = new InMemoryCancellationToken(new CancellationToken(canceled = false))
        interface ICloudCancellationToken with
            member x.IsCancellationRequested: bool = token.IsCancellationRequested
            member x.LocalToken: CancellationToken = token

    type internal InMemoryCancellationTokenSource(source : CancellationTokenSource) =
        new () = new InMemoryCancellationTokenSource(new CancellationTokenSource())
        interface ICloudCancellationTokenSource with
            member x.Dispose(): Async<unit> = async {source.Cancel()}
            member x.Cancel(): unit = source.Cancel()
            member x.Token: ICloudCancellationToken = new InMemoryCancellationToken(source.Token) :> _