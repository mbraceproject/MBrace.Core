namespace MBrace.SampleRuntime

open System
open System.Runtime.Serialization

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

[<AutoSerializable(true)>]
type ActorCloudLogger private (target : ActorRef<string * DateTime>) =

    member __.CreateLogger (worker : IWorkerId, currentJob : CloudJob) =
        { new ICloudLogger with
            member __.Log (message : string) =
                let message = sprintf "User Log[Worker:%s, Process:%s, Job:%s]: %s" worker.Id currentJob.TaskEntry.Info.Id currentJob.Id message
                target <-- (message, DateTime.Now)
        }

    static member Init(logger : ISystemLogger) =
        let ref =
            Actor.Stateless (fun (m,d) -> async { return logger.LogEntry(LogLevel.Info, d, m) })
            |> Actor.Publish
            |> Actor.ref

        new ActorCloudLogger(ref)