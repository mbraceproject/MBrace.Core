namespace MBrace.Thespian.Runtime

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private CounterMessage =
    | Increment of IReplyChannel<int64>
    | GetValue of IReplyChannel<int64>

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
    static member Create(init : int64) =
        let behaviour count msg = async {
            match msg with
            | Increment rc ->
                do! rc.Reply (count + 1L)
                return (count + 1L)
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
        member x.CreateCounter(initialValue: int64): Async<ICloudCounter> =
            factory.RequestResource(fun () -> ActorCounter.Create initialValue :> ICloudCounter)