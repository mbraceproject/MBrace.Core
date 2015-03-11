namespace MBrace

open System
open System.Threading
open System.Threading.Tasks

/// Denotes a reference to a worker node in the cluster.
type IWorkerRef =
    inherit IComparable
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string
    /// Worker processor count
    abstract ProcessorCount : int

/// Denotes a task that is being executed in the cluster.
type ICloudTask<'T> =
    /// Unique task identifier
    abstract Id : string
    /// Gets a TaskStatus enumeration indicating the current task state.
    abstract Status : TaskStatus
    /// Gets a boolean indicating that the task has completed successfully.
    abstract IsCompleted : bool
    /// Gets a boolean indicating that the task has completed with fault.
    abstract IsFaulted : bool
    /// Gets a boolean indicating that the task has been canceled.
    abstract IsCanceled : bool
    /// Awaits task for completion, returning its eventual result
    abstract AwaitResult : ?timeoutMilliseconds:int -> Local<'T>
    /// Rreturns the task result if completed or None if still pending.
    abstract TryGetResult : unit -> Local<'T option>
    /// Synchronously gets the task result, blocking until it completes.
    abstract Result : 'T