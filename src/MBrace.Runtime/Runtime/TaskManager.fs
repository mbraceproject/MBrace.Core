namespace MBrace.Runtime

open System
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

/// Cloud task status
type CloudTaskStatus =
    /// Task posted to cluster for execution
    | Posted
    /// Root work item for task dequeued
    | Dequeued 
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
with
    /// Gets the corresponding System.Threading.TaskStatus enumeration
    member t.TaskStatus =
        match t with
        | Posted -> TaskStatus.Created
        | Dequeued -> TaskStatus.WaitingToRun
        | Running -> TaskStatus.Running
        | Faulted | UserException -> TaskStatus.Faulted
        | Completed -> TaskStatus.RanToCompletion
        | Canceled -> TaskStatus.Canceled


/// Task result container
[<NoEquality; NoComparison>]
type TaskResult =
    /// Task completed with value
    | Completed of obj
    /// Task failed with user exception
    | Exception of ExceptionDispatchInfo
    //// Task canceled
    | Cancelled of OperationCanceledException
with
    /// Gets the contained result value,
    /// will be thrown if exception.
    member inline r.Value : obj =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled e -> raise e

/// Cloud task metadata
[<NoEquality; NoComparison>]
type CloudTaskInfo =
    {
        /// User-specified task name
        Name : string option
        /// Cancellation token source for task
        CancellationTokenSource : ICloudCancellationTokenSource
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Additional user-supplied resources for cloud task.
        AdditionalResources : ResourceRegistry option
        /// Task return type in pretty printed form
        ReturnTypeName : string
        /// Task return type in pickled form
        ReturnType : Pickle<Type>
    }

/// Cloud execution time metadata
[<NoEquality; NoComparison>]
type ExecutionTime =
    /// Task has not been started
    | NotStarted
    /// Task has started but not completed yet
    | Started of startTime:DateTime * executionTime:TimeSpan
    /// Task has completed
    | Finished of startTime:DateTime * executionTime:TimeSpan * completionTime:DateTime

/// Cloud task execution state record
[<NoEquality; NoComparison>]
type CloudTaskState =
    {
        /// Task metadata
        Info : CloudTaskInfo
        /// Task execution status
        Status : CloudTaskStatus
        /// Task execution time
        ExecutionTime : ExecutionTime
        /// Max number of concurrently executing work items for task.
        MaxActiveWorkItemCount : int
        /// Number of work items currently executing for task.
        ActiveWorkItemCount : int
        /// Number of work items completed for task.
        CompletedWorkItemCount : int
        /// Number of times jobs have been faulted.
        FaultedWorkItemCount : int
        /// Total number of work items spawned by task.
        TotalWorkItemCount : int
    }

/// Cloud task completion source abstraction
type ICloudTaskCompletionSource =
    /// Unique Cloud Task identifier
    abstract Id : string
    /// Gets cloud task metadata
    abstract Info : CloudTaskInfo
    /// Asynchronously fetches current task state
    abstract GetState : unit -> Async<CloudTaskState>

    /// <summary>
    ///     Asynchronously awaits for the task result.
    /// </summary>
    abstract AwaitResult : unit -> Async<TaskResult>

    /// <summary>
    ///     Asynchronously checks if task result has been set.
    ///     Returns None if result has not been set.
    /// </summary>
    abstract TryGetResult : unit -> Async<TaskResult option>

    /// <summary>
    ///     Asynchronously attempts to set task result for entry.
    ///     Returns true if setting was successful.
    /// </summary>
    /// <param name="result">Result value to be set.</param>
    /// <param name="workerId">Worker identifier for result setter.</param>
    abstract TrySetResult : result:TaskResult * workerId:IWorkerId -> Async<bool>

    /// <summary>
    ///     Declares task execution status.
    /// </summary>
    /// <param name="status">Declared status.</param>
    abstract DeclareStatus : status:CloudTaskStatus -> Async<unit>

    /// <summary>
    ///     Increments work item count for provided task.
    /// </summary>
    abstract IncrementJobCount : unit -> Async<unit>

    /// <summary>
    ///     Asynchronously increments the faulted work item count for task.
    /// </summary>
    abstract IncrementFaultedWorkItemCount : unit -> Async<unit>

    /// <summary>
    ///     Decrements work item count for provided task.
    /// </summary>
    abstract IncrementCompletedWorkItemCount : unit -> Async<unit>

/// Cloud task manager object
type ICloudTaskManager =
    
    /// <summary>
    ///     Request a new task from cluster state.
    /// </summary>
    /// <param name="info">User-supplied cloud task metadata.</param>
    abstract CreateTask : info:CloudTaskInfo -> Async<ICloudTaskCompletionSource>

    /// <summary>
    ///     Gets a cloud task entry for provided task id.
    /// </summary>
    /// <param name="taskId">Task id to be looked up.</param>
    abstract TryGetTask : taskId:string -> Async<ICloudTaskCompletionSource option>

    /// <summary>
    ///     Asynchronously fetches task execution state for all tasks currently in task.
    /// </summary>
    abstract GetAllTasks : unit -> Async<ICloudTaskCompletionSource []>

    /// <summary>
    ///     Deletes task info of given id.
    /// </summary>
    /// <param name="taskId">Cloud task identifier.</param>
    abstract Clear : taskId:string -> Async<unit>

    /// <summary>
    ///     Deletes all task info from current runtime.
    /// </summary>
    abstract ClearAllTasks : unit -> Async<unit>