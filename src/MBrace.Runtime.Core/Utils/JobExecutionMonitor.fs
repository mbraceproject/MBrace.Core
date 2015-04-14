namespace MBrace.Runtime.Utils

open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

// Jobs are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// JobExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

/// Provides a mechanism for cooperative task execution monitoring.
[<AutoSerializable(false)>]
type JobExecutionMonitor () =
    let tcs = TaskCompletionSource<unit> ()
    static let fromContext (ctx : ExecutionContext) = ctx.Resources.Resolve<JobExecutionMonitor> ()

    member __.Task = tcs.Task
    member __.TriggerFault (e : exn) = tcs.TrySetException e |> ignore
    member __.TriggerCompletion () = tcs.TrySetResult () |> ignore

    /// Runs a single threaded, synchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectSync ctx (f : unit -> unit) : unit =
        let tem = fromContext ctx
        try f () with e -> tem.TriggerFault e |> ignore

    /// Runs an asynchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectAsync ctx (f : Async<unit>) : unit =
        let tem = fromContext ctx
        Async.StartWithContinuations(f, ignore, tem.TriggerFault, ignore)   

    /// Triggers task completion on the contextual TaskExecutionMonitor
    static member TriggerCompletion ctx =
        let tem = fromContext ctx in tem.TriggerCompletion () |> ignore

    /// Triggers task fault on the contextual TaskExecutionMonitor
    static member TriggerFault (ctx, e) =
        let tem = fromContext ctx in tem.TriggerFault e |> ignore

    /// Asynchronously await completion of provided TaskExecutionMonitor
    static member AwaitCompletion (tem : JobExecutionMonitor) = async {
        try
            return! Async.AwaitTask tem.Task
        with :? System.AggregateException as e when e.InnerException <> null ->
            return! Async.Raise e.InnerException
    }