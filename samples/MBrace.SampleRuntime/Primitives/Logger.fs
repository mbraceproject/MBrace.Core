namespace MBrace.SampleRuntime

open Nessos.Thespian
open MBrace.Core.Internals

type Logger private (target : ActorRef<string>) =
    interface ICloudLogger with member __.Log txt = target <-- txt
    static member Init(logger : ICloudLogger) =
        let ref =
            Actor.Stateless (fun msg -> async { return logger.Log msg })
            |> Actor.Publish
            |> Actor.ref

        new Logger(ref)