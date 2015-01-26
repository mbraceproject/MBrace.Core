namespace MBrace

open System

/// Scheduling context for currently executing cloud process.
type SchedulingContext =
    /// Current thread scheduling context
    | Sequential
    /// Thread pool scheduling context
    | ThreadParallel
    /// Distributed scheduling context
    | Distributed

/// Denotes a reference to a worker node in the cluster
type IWorkerRef =
    inherit IComparable
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string

namespace MBrace.Runtime

// Distributed runtime provider definition
// Cloud workflows actuating parallelism should
// be passed an instance of ICloudRuntimeProvider in their
// ExecutionContext.

open MBrace

/// <summary>
///     Executing runtime abstraction.
/// </summary>
type ICloudRuntimeProvider =

    /// Get cloud process identifier
    abstract ProcessId : string
    /// Get cloud task identifier
    abstract TaskId : string
    /// Get all available workers in cluster
    abstract GetAvailableWorkers : unit -> Async<IWorkerRef []>
    /// Gets currently running worker
    abstract CurrentWorker : IWorkerRef
    /// Gets the current logger instance.
    abstract Logger : ICloudLogger

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    abstract FaultPolicy : FaultPolicy

    /// <summary>
    ///     Creates a new scheduler instance with updated fault policy.
    /// </summary>
    /// <param name="newPolicy">new fault policy.</param>
    abstract WithFaultPolicy : newPolicy:FaultPolicy -> ICloudRuntimeProvider

    /// <summary>
    ///     Creates a new scheduler instance with updated scheduling context
    /// </summary>
    /// <param name="newContext">new scheduling context</param>
    abstract WithSchedulingContext : newContext:SchedulingContext -> ICloudRuntimeProvider

    /// <summary>
    ///     Gets the current scheduling context.
    /// </summary>
    abstract SchedulingContext : SchedulingContext

    /// Specifies whether runtime supports submission of tasks to specific worker nodes
    abstract IsTargetedWorkerSupported : bool

    /// <summary>
    ///     Parallel fork/join implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleParallel : computations:seq<Cloud<'T> * IWorkerRef option> -> Cloud<'T []>

    /// <summary>
    ///     Parallel nondeterministic choice implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleChoice : computations:seq<Cloud<'T option> * IWorkerRef option> -> Cloud<'T option>

    /// <summary>
    ///     Start a new computation as a child task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="target">Explicitly specify a target worker for execution.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    abstract ScheduleStartChild : workflow:Cloud<'T> * ?target:IWorkerRef * ?timeoutMilliseconds:int -> Cloud<Cloud<'T>>