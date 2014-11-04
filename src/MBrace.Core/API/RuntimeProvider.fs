namespace Nessos.MBrace.Runtime

// Distributed runtime provider definition
// Cloud workflows actuating parallelism should
// be passed an instance of IRuntimeProvider in their
// ExecutionContext.

open Nessos.MBrace

/// <summary>
///     Abstract logger.
/// </summary>
type ICloudLogger =
    /// <summary>
    ///     Log a new message to the execution context.
    /// </summary>
    /// <param name="entry">Entry to be logged.</param>
    abstract Log : entry:string -> unit

/// <summary>
///     Executing runtime abstraction.
/// </summary>
type IRuntimeProvider =

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
    ///     Creates a new scheduler instance with updated scheduling context
    /// </summary>
    /// <param name="newContext">new scheduling context</param>
    abstract WithSchedulingContext : newContext:SchedulingContext -> IRuntimeProvider

    /// <summary>
    ///     Gets the current scheduling context.
    /// </summary>
    abstract SchedulingContext : SchedulingContext

    /// <summary>
    ///     Parallel fork/join implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed.</param>
    abstract ScheduleParallel : computations:seq<Cloud<'T>> -> Cloud<'T []>

    /// <summary>
    ///     Parallel nondeterministic choice implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed.</param>
    abstract ScheduleChoice : computations:seq<Cloud<'T option>> -> Cloud<'T option>

    /// <summary>
    ///     Start a new computation as a child task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="target">Explicitly specify a target worker for execution.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    abstract ScheduleStartChild : workflow:Cloud<'T> * ?target:IWorkerRef * ?timeoutMilliseconds:int -> Cloud<Cloud<'T>>