namespace MBrace.Runtime

open System

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

/// Cloud task status
[<NoEquality; NoComparison>]
type CloudTaskStatus =
    /// Task posted to cluster for execution
    | Posted
    /// Task being executed by the cluster
    | Running
    /// Task encountered a cluster fault
    | Faulted
    /// Task completed successfully
    | Completed
    /// Task completed with user exception
    | UserException
    /// Task cancelled by user
    | Canceled

/// Cloud task metadata
[<NoEquality; NoComparison>]
type CloudTaskInfo =
    {
        /// Cloud task unique identifier
        Id : string
        /// User-specified task name
        Name : string option
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Task return type
        Type : string
    }

/// Cloud task execution state record
[<NoEquality; NoComparison>]
type CloudTaskState =
    {
        /// Task metadata
        Info : CloudTaskInfo
        /// Task execution status
        Status : CloudTaskStatus
        /// Task execution time : Start time * Execution time
        ExecutionTime : (DateTime * TimeSpan) option
        /// Max number of concurrently executing jobs for task.
        MaxActiveJobCount : int
        /// Number of jobs currently executing for task.
        ActiveJobCount : int
        /// Total number of jobs spawned by task.
        TotalJobCount : int
    }

/// Defines a distributed task completion source.
type ICloudTaskCompletionSource =
    /// Task metadata object.
    abstract Info : CloudTaskInfo

    /// Type of the TaskCompletionSource
    abstract Type : Type

    /// Gets the cancellation token source for the task.
    abstract CancellationTokenSource : ICloudCancellationTokenSource

/// Defines a distributed task completion source.
type ICloudTaskCompletionSource<'T> =
    inherit ICloudTaskCompletionSource
    /// Gets the underlying cloud task for the task completion source.
    abstract Task : ICloudTask<'T>

    /// <summary>
    ///     Asynchronously sets a completed result for the task.
    /// </summary>
    /// <param name="value">Task completion value.</param>
    abstract SetCompleted : value:'T -> Async<unit>

    /// <summary>
    ///     Asynchronously sets the the result to be an exception.
    /// </summary>
    /// <param name="edi">Exception dispatch info to be submitted.</param>
    abstract SetException : edi:ExceptionDispatchInfo -> Async<unit>

    /// <summary>
    ///     Asynchronously declares the the result to be cancelled.
    /// </summary>
    /// <param name="edi">Exception dispatch info to be submitted.</param>
    abstract SetCancelled : exn:OperationCanceledException -> Async<unit>

/// Cloud task manager object
type ICloudTaskManager =
    
    /// <summary>
    ///     Request a new task from cluster state.
    /// </summary>
    /// <param name="dependencies">Declared Vagabond dependencies for task.</param>
    /// <param name="taskName">User-supplied task name.</param>
    /// <param name="cancellationTokenSource">Cancellation token source for task.</param>
    abstract RequestTaskCompletionSource<'T> : dependencies:AssemblyId[] * cancellationTokenSource:ICloudCancellationTokenSource * ?taskName:string -> Async<ICloudTaskCompletionSource<'T>>

    /// <summary>
    ///     Request an alredy existing task by its unique identifier.
    /// </summary>
    /// <param name="taskId">Task unique identifier.</param>
    abstract GetTaskCompletionSourceById : taskId:string -> Async<ICloudTaskCompletionSource>
    
    /// <summary>
    ///     Declares task execution status.
    /// </summary>
    /// <param name="taskId">Task unique identifier.</param>
    /// <param name="status">Declared status.</param>
    abstract DeclareStatus : taskId:string * status:CloudTaskStatus -> Async<unit>

    /// <summary>
    ///     Increments job count for provided task.
    /// </summary>
    /// <param name="taskId">Task unique identifier.</param>
    abstract IncrementJobCount : taskId:string -> Async<unit>

    /// <summary>
    ///     Decrements job count for provided task.
    /// </summary>
    /// <param name="taskId">Task unique identifier.</param>
    abstract DecrementJobCount : taskId:string -> Async<unit>

    /// <summary>
    ///     Asynchronously fetches execution state for task of provided id.
    /// </summary>
    /// <param name="taskId">Task unique identifier.</param>
    abstract GetTaskState : taskId:string -> Async<CloudTaskState>

    /// <summary>
    ///     Asynchronously fetches task execution state for all tasks currently in task.
    /// </summary>
    abstract GetAllTasks : unit -> Async<CloudTaskState []>

    /// <summary>
    ///     Deletes all task info from current runtime.
    /// </summary>
    abstract ClearAllTasks : unit -> Async<unit>

    /// <summary>
    ///     Deletes task info of given id.
    /// </summary>
    /// <param name="taskId">Task identifier.</param>
    abstract Clear : taskId:string -> Async<unit>