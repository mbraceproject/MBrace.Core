namespace MBrace.Thespian.Runtime

open Nessos.Thespian

open MBrace.Runtime

/// Distributable, actor-based system logger
[<Sealed; AutoSerializable(true)>]
type ActorLogger private (ref : ActorRef<SystemLogEntry>) =
    interface ISystemLogger with
        member __.LogEntry(e : SystemLogEntry) = ref <-- e

    /// Creates an actor logger that writes to the underlying implementation.
    static member Create(underlying : ISystemLogger) =
        let behaviour (e : SystemLogEntry) = async { return underlying.LogEntry e }

        let aref =
            Actor.Stateless behaviour
            |> Actor.Publish
            |> Actor.ref

        new ActorLogger(aref)