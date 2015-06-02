namespace MBrace.SampleRuntime

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private CounterMessage =
    | IncreaseBy of int * IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed counter implementation
type Counter private (source : ActorRef<CounterMessage>) =
    interface ICloudCounter with
        member __.Increment () = source <!- fun ch -> IncreaseBy(1,ch)
        member __.Decrement () = source <!- fun ch -> IncreaseBy(-1,ch)
        member __.Value = source <!- GetValue
        member __.Dispose () = async.Zero()

    /// Initialize a new latch instance in the current process
    static member Init(init : int) =
        let behaviour count msg = async {
            match msg with
            | IncreaseBy (i, rc) ->
                do! rc.Reply (count + i)
                return (count + i)
            | GetValue rc ->
                do! rc.Reply count
                return count
        }

        let ref =
            Actor.Stateful init behaviour
            |> Actor.Publish
            |> Actor.ref

        new Counter(ref)