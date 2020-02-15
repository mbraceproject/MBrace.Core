namespace MBrace.Thespian.Runtime

open Nessos.Thespian
open MBrace.Library

open MBrace.Runtime
open MBrace.Runtime.Components

type MarshalledLogger private (actorRef : ActorRef<SystemLogEntry>) =
    interface ISystemLogger with
        member __.LogEntry (entry: SystemLogEntry) = actorRef <-- entry

    static member Create(local : ISystemLogger) =
        let actorRef = 
            Behavior.stateless (local.LogEntry >> async.Return)
            |> Actor.bind
            |> Actor.Publish
            |> Actor.ref

        new MarshalledLogger(actorRef)