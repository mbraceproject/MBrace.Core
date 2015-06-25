namespace MBrace.Thespian.Runtime

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

/// An actor logger implementation that accepts 
/// log entries from remote processes
[<AutoSerializable(true)>]
type ActorCloudLogger private (target : ActorRef<string * DateTime>) =

    /// <summary>
    ///     Gets an ICloudLogger implementation that is bound to specific cloud job.
    /// </summary>
    /// <param name="worker">Originating worker id.</param>
    /// <param name="currentJob">Current job being executed.</param>
    member __.GetCloudLogger (worker : IWorkerId, currentJob : CloudJob) =
        { new ICloudLogger with
            member __.Log (message : string) =
                let message = sprintf "[Worker:%s, Process:%s, Job:%s]: %s" worker.Id currentJob.TaskEntry.Id currentJob.Id message
                target <-- (message, DateTime.Now)
        }

    /// <summary>
    ///     Creates a logger actor in the local process that pushes
    ///     entries to the specified target logger.s
    /// </summary>
    /// <param name="targetLogger">Target logger to push log entries to.</param>
    static member Create(target : ISystemLogger) =
        let ref =
            Actor.Stateless (fun (m,d) -> async { return target.LogEntry(LogLevel.Info, d, m) })
            |> Actor.Publish
            |> Actor.ref

        new ActorCloudLogger(ref)