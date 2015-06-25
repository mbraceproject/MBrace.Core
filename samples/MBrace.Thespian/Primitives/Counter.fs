namespace MBrace.Thespian

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private CounterMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed counter implementation
type Counter private (source : ActorRef<CounterMessage>) =
    interface ICloudCounter with
        member __.Increment () = source <!- Increment
        member __.Value = source <!- GetValue
        member __.Dispose () = async.Zero()

    /// Initialize a new latch instance in the current process
    static member Init(init : int) =
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

        new Counter(ref)