namespace MBrace

open System
open System.Threading
open System.Threading.Tasks

/// Scheduling context for currently executing cloud process.
type SchedulingContext =
    /// Current thread scheduling context
    | Sequential
    /// Thread pool scheduling context
    | ThreadParallel
    /// Distributed scheduling context
    | Distributed

/// Denotes a reference to a worker node in the cluster.
type IWorkerRef =
    inherit IComparable
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string

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
    abstract AwaitResult : ?timeoutMilliseconds:int -> Cloud<'T>
    /// Returns the task result result if completed or None if still pending.
    abstract TryGetResult : unit -> Cloud<'T option>
    /// Synchronously gets the task result.
    abstract Result : 'T