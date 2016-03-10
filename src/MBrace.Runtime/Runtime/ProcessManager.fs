namespace MBrace.Runtime

open System
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

/// Task result container
[<NoEquality; NoComparison>]
type CloudProcessResult =
    /// Task completed with value
    | Completed of obj
    /// Task failed with user exception
    | Exception of ExceptionDispatchInfo
    //// Task canceled
    | Cancelled of OperationCanceledException

    /// Gets the contained result value,
    /// will be thrown if exception.
    member inline r.Value : obj =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled e -> raise e

/// Cloud process metadata
[<NoEquality; NoComparison>]
type CloudProcessInfo =
    {
        /// User-specified cloud process name
        Name : string option
        /// Cancellation token source for cloud process
        CancellationTokenSource : CloudCancellationToken
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Additional user-supplied resources for cloud process.
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
    | Started of startTime:DateTimeOffset * executionTime:TimeSpan
    /// Task has completed
    | Finished of startTime:DateTimeOffset * executionTime:TimeSpan

/// Cloud process execution state record
[<NoEquality; NoComparison>]
type CloudProcessState =
    {
        /// Task metadata
        Info : CloudProcessInfo
        /// Task execution status
        Status : CloudProcessStatus
        /// Task execution time
        ExecutionTime : ExecutionTime
        /// Number of work items currently executing for cloud process.
        ActiveWorkItemCount : int
        /// Number of work items completed for cloud process.
        CompletedWorkItemCount : int
        /// Number of times work items have been faulted.
        FaultedWorkItemCount : int
        /// Total number of work items spawned by cloud process.
        TotalWorkItemCount : int
    }

/// Cloud process completion source abstraction
type ICloudProcessEntry =
    /// Unique Cloud Task identifier
    abstract Id : string
    /// Gets cloud process metadata
    abstract Info : CloudProcessInfo
    /// Asynchronously fetches current cloud process state
    abstract GetState : unit -> Async<CloudProcessState>

    /// <summary>
    ///     Asynchronously awaits for the cloud process result.
    /// </summary>
    abstract AwaitResult : unit -> Async<CloudProcessResult>

    /// Asynchronously awaits until cloud process has completed
    abstract WaitAsync : unit -> Async<unit>

    /// <summary>
    ///     Asynchronously checks if cloud process result has been set.
    ///     Returns None if result has not been set.
    /// </summary>
    abstract TryGetResult : unit -> Async<CloudProcessResult option>

    /// <summary>
    ///     Asynchronously attempts to set cloud process result for entry.
    ///     Returns true if setting was successful.
    /// </summary>
    /// <param name="result">Result value to be set.</param>
    /// <param name="workerId">Worker identifier for result setter.</param>
    abstract TrySetResult : result:CloudProcessResult * workerId:IWorkerId -> Async<bool>

    /// <summary>
    ///     Declares cloud process execution status.
    /// </summary>
    /// <param name="status">Declared status.</param>
    abstract DeclareStatus : status:CloudProcessStatus -> Async<unit>

    /// <summary>
    ///     Increments work item count for provided cloud process.
    /// </summary>
    abstract IncrementWorkItemCount : unit -> Async<unit>

    /// <summary>
    ///     Asynchronously increments the faulted work item count for cloud process.
    /// </summary>
    abstract IncrementFaultedWorkItemCount : unit -> Async<unit>

    /// <summary>
    ///     Decrements work item count for provided cloud process.
    /// </summary>
    abstract IncrementCompletedWorkItemCount : unit -> Async<unit>

/// Cloud process manager object
type ICloudProcessManager =
    
    /// <summary>
    ///     Request a new cloud process from cluster state.
    /// </summary>
    /// <param name="info">User-supplied cloud process metadata.</param>
    abstract StartProcess : info:CloudProcessInfo -> Async<ICloudProcessEntry>

    /// <summary>
    ///     Gets a cloud process entry for provided cloud process id.
    /// </summary>
    /// <param name="procId">Task id to be looked up.</param>
    abstract TryGetProcessById : procId:string -> Async<ICloudProcessEntry option>

    /// <summary>
    ///     Asynchronously fetches cloud process execution state for all tasks currently in cloud process.
    /// </summary>
    abstract GetAllProcesses : unit -> Async<ICloudProcessEntry []>

    /// <summary>
    ///     Deletes cloud process info of given id.
    /// </summary>
    /// <param name="procId">Cloud process identifier.</param>
    abstract ClearProcess : procId:string -> Async<unit>

    /// <summary>
    ///     Deletes all cloud process info from current runtime.
    /// </summary>
    abstract ClearAllProcesses : unit -> Async<unit>