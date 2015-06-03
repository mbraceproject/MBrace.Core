namespace MBrace.SampleRuntime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

/// Provides facility for remotely deploying resources
[<AutoSerializable(true)>]
type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =

    let getResource (f : unit -> 'T) = async {
        let ctor () = f () :> obj
        let! resource = source <!- fun ch -> RequestResource(ctor, ch)
        return resource :?> 'T
    }

    /// <summary>
    ///     Executes a computation in the remote party and returns the result.
    /// </summary>
    /// <param name="factory">Factory method to be executed.</param>
    member __.RequestResource<'T>(factory : unit -> 'T) = getResource factory
    member __.RequestCounter(initial:int) = getResource (fun () -> Counter.Init(initial))
    member __.RequestResultAggregator<'T>(capacity:int) = getResource (fun () -> ResultAggregator<'T>.Init capacity)
    member __.RequestTaskCompletionSource<'T>() = getResource(fun () -> TaskCompletionSource<'T>.Init())
    member __.RequestCancellationEntry() = getResource(fun () -> CancellationEntry.Init())

    interface ICancellationEntryFactory with
        member x.CreateCancellationEntry() = async {
            let! e = x.RequestCancellationEntry() in return e :> ICancellationEntry
        }
        
        member x.TryCreateLinkedCancellationEntry(parents: ICancellationEntry []) = async {
            let parents = parents |> Array.map unbox<CancellationEntry>
            let! e = x.RequestCancellationEntry()
            let! results =
                parents
                |> Seq.map (fun p -> p.RegisterChild e)
                |> Async.Parallel

            if Array.forall id results 
            then return Some(e :> ICancellationEntry)
            else let! _ = e.Cancel() in return None
        }

    /// <summary>
    ///     Creates an actor that accepts and deploys factory methods from remote senders.
    /// </summary>
    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = try ctor () |> Choice1Of2 with e -> Choice2Of2 e
            match r with
            | Choice1Of2 res -> do! rc.Reply res
            | Choice2Of2 e -> do! rc.ReplyWithException e
        }

        let ref =
            Actor.Stateless behavior
            |> Actor.Publish
            |> Actor.ref

        new ResourceFactory(ref)