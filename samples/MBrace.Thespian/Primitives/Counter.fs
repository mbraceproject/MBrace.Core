namespace MBrace.Thespian.Runtime

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private CounterMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed counter implementation
type ActorCounter private (source : ActorRef<CounterMessage>) =
    interface ICloudCounter with
        member __.Increment () = source <!- Increment
        member __.Value = source <!- GetValue
        member __.Dispose () = async.Zero()

    /// <summary>
    ///     Initializes an actor counter instance in the local process.
    /// </summary>
    /// <param name="init">Initial counter value.</param>
    static member Create(init : int) =
        let behaviour count msg = async {
            match msg with
            | Increment rc ->
                do! rc.Reply (count + 1)
                return (count + 1)
            | GetValue rc ->
                do! rc.Reply count
                return count
        }

        let ref =
            Actor.Stateful init behaviour
            |> Actor.Publish
            |> Actor.ref

        new ActorCounter(ref)


type ActorCounterFactory(factory : ResourceFactory) =
    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int): Async<ICloudCounter> =
            factory.RequestResource(fun () -> ActorCounter.Create initialValue :> ICloudCounter)