namespace MBrace.Runtime

open System

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils.PerformanceMonitor

/// Worker operation status
[<NoEquality; NoComparison>]
type WorkerStatus =
    /// Worker dequeueing jobs normally
    | Running
    /// Worker has been stopped manually
    | Stopped
    /// Error dequeueing jobs
    | QueueFault of ExceptionDispatchInfo

/// Worker state object
[<NoEquality; NoComparison>]
type WorkerState =
    {
        /// Worker operation status
        Status : WorkerStatus
        /// Current number of executing jobs
        CurrentJobCount : int
        /// Maximum number of executing jobs
        MaxJobCount : int
    }

/// Worker manager abstraction
type IWorkerManager =

    /// <summary>
    /// Returns true if supplied worker implementation
    /// is valid and belonging to this cluster.
    /// </summary>
    /// <param name="target">worker ref to be examined.</param>
    abstract IsValidTargetWorker : target:IWorkerRef -> Async<bool>

    /// <summary>
    ///     Subscribe a new worker instance to the cluster.
    /// </summary>
    /// <param name="worker">Worker instance to be subscribed.</param>
    /// <param name="worker">Initial worker state.</param>
    /// <returns>Unsubscribe disposable.</returns>
    abstract SubscribeWorker : worker:IWorkerRef * initial:WorkerState -> Async<IDisposable>

    /// <summary>
    ///     Asynchronously declares worker state for provided worker.
    /// </summary>
    /// <param name="worker">Worker to be updated.</param>
    /// <param name="state">State for worker.</param>
    abstract DeclareWorkerState : worker:IWorkerRef * state:WorkerState -> Async<unit>

    /// <summary>
    ///     Asynchronously submits node performance metrics for provided worker.
    /// </summary>
    /// <param name="worker">Worker declaring performance metrics.</param>
    /// <param name="perf">Performance metrics for given worker.</param>
    abstract SubmitPerformanceMetrics : worker:IWorkerRef * perf:NodePerformanceInfo -> Async<unit>

    /// Asynchronously requests node performance metrics for all nodes.
    abstract GetAvailableWorkers : unit -> Async<(IWorkerRef * WorkerState * NodePerformanceInfo) []>