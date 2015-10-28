namespace MBrace.Core

open System
open System.Diagnostics
open System.Runtime.CompilerServices
open System.Threading.Tasks
open System.ComponentModel

open MBrace.Core.Internals

/// Enumeration specifying the execution status for a CloudProcess
type CloudProcessStatus =
    /// Process posted to cluster for execution.
    | Created               = 1
    /// Process has been allocated and awaits execution.
    | WaitingToRun          = 2
    /// Process is being executed by the runtime.
    | Running               = 4
    /// Process encountered a runtime fault.
    | Faulted               = 8
    /// Process has completed successfully.
    | Completed             = 16
    /// Process has completed with an uncaught user exception.
    | UserException         = 32
    /// Process has been cancelled by the user.
    | Canceled              = 64

/// Denotes a cloud process that is being executed in the cluster.
type ICloudProcess =
    /// Unique cloud process identifier
    abstract Id : string
    /// Gets the cancellation corresponding to the Task instance
    abstract CancellationToken : ICloudCancellationToken
    /// Gets a TaskStatus enumeration indicating the current cloud process state.
    abstract Status : CloudProcessStatus
    /// Gets a boolean indicating that the cloud process has completed successfully.
    abstract IsCompleted : bool
    /// Gets a boolean indicating that the cloud process has completed with fault.
    abstract IsFaulted : bool
    /// Gets a boolean indicating that the cloud process has been canceled.
    abstract IsCanceled : bool
    /// Synchronously gets the cloud process result, blocking until it completes.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract ResultBoxed : obj
    /// <summary>
    ///     Asynchronously waits for the cloud process to complete.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract WaitAsync : ?timeoutMilliseconds:int -> Async<unit>
    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResultBoxedAsync : ?timeoutMilliseconds:int -> Async<obj>
    /// Returns the cloud process result if completed or None if still pending.
    abstract TryGetResultBoxedAsync : unit -> Async<obj option>

/// Denotes a cloud process that is being executed in the cluster.
type ICloudProcess<'T> =
    inherit ICloudProcess
    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResultAsync : ?timeoutMilliseconds:int -> Async<'T>
    /// Returns the cloud process result if completed or None if still pending.
    abstract TryGetResultAsync : unit -> Async<'T option>
    /// Synchronously gets the cloud process result, blocking until it completes.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract Result : 'T

/// Cloud Process helper methods
type CloudProcessHelpers =

    /// <summary>
    ///     Asynchronously waits until one of the given processes completes.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAny([<ParamArray>] processes : ICloudProcess []) : Async<ICloudProcess> = async {
        let await (p : ICloudProcess) = Async.StartAsTask(async { let! _ = p.WaitAsync() in return p })
        let awaited = processes |> Seq.map await |> Task.WhenAny
        return! awaited.ContinueWith(fun (t:Task<Task<_>>) -> t.Result.Result) |> Async.AwaitTaskCorrect
    }

    /// <summary>
    ///     Asynchronously waits until one of the given processes completes.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAny([<ParamArray>] processes : ICloudProcess<'T> []) : Async<ICloudProcess<'T>> = async {
        let! awaited = CloudProcessHelpers.WhenAny(processes |> Array.map unbox<ICloudProcess>)
        return awaited :?> ICloudProcess<'T>
    }

    /// <summary>
    ///     Asynchronously waits until all of the given processes complete.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAll([<ParamArray>] processes : ICloudProcess []) : Async<unit> = async {
        let await (p : ICloudProcess) = Async.StartAsTask(async { let! _ = p.WaitAsync() in return p })
        let awaited = processes |> Seq.map await |> Task.WhenAll
        let! _ = Async.AwaitTaskCorrect awaited
        return ()
    }

[<Extension; EditorBrowsable(EditorBrowsableState.Never)>]
type CloudProcessExtensions =

    /// Returns the cloud process result if completed or None if still pending.
    [<Extension>]
    static member TryGetResultBoxed(this : ICloudProcess) : obj option = 
        this.TryGetResultBoxedAsync() |> Async.RunSync

    /// Returns the cloud process result if completed or None if still pending.
    [<Extension>]
    static member TryGetResult(this : ICloudProcess<'T>) : 'T option = 
        this.TryGetResultAsync() |> Async.RunSync

    /// Waits for the cloud process to complete.
    [<Extension>]
    static member Wait(this : ICloudProcess) : unit =
        this.WaitAsync() |> Async.RunSync

    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    [<Extension>]
    static member AwaitResultBoxed(this : ICloudProcess, ?timeoutMilliseconds) : LocalCloud<obj> = 
        this.AwaitResultBoxedAsync(?timeoutMilliseconds = timeoutMilliseconds) |> ofAsync |> mkLocal

    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    [<Extension>]
    static member AwaitResult(this : ICloudProcess<'T>, ?timeoutMilliseconds : int) : LocalCloud<'T> = 
        this.AwaitResultAsync(?timeoutMilliseconds = timeoutMilliseconds) |> ofAsync |> mkLocal