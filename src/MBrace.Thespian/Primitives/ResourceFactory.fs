namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

/// Defines a reference to an actor implementation that accepts remote requests
/// for factory initializations. Can be used for quickly spawning actors remotely.
[<AutoSerializable(true)>]
type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =

    /// <summary>
    ///     Submits a factory for remote initialization.
    /// </summary>
    /// <param name="factory">Factory method to be executed.</param>
    member __.RequestResource<'T>(factory : unit -> 'T) = async {
        let ctor () = factory () :> obj
        let! resource = source <!- fun ch -> RequestResource(ctor, ch)
        return resource :?> 'T
    }

    /// Creates a resource factory initializer running in the local process.
    static member Create () =
        let init (RequestResource(ctor,rc)) = async {
            let r = try ctor () |> Choice1Of2 with e -> Choice2Of2 e
            match r with
            | Choice1Of2 res -> do! rc.Reply res
            | Choice2Of2 e -> do! rc.ReplyWithException e
        }

        let ref =
            Actor.Stateless (fun msg -> async { Async.Start (init msg) })
            |> Actor.Publish
            |> Actor.ref

        new ResourceFactory(ref)