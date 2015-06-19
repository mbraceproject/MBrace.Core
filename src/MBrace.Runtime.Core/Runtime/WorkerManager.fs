namespace MBrace.Runtime

open System

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils.PerformanceMonitor

/// Runtime provided worker identifier
/// must implement equality and comparison semantics
type IWorkerId =
    inherit IComparable
    /// Worker identifier
    abstract Id : string

/// Worker operation status
[<NoEquality; NoComparison>]
type WorkerStatus =
    /// Worker dequeueing jobs normally
    | Running
    /// Worker has been stopped manually
    | Stopped
    /// Error dequeueing jobs
    | QueueFault of ExceptionDispatchInfo
with
    override s.ToString() =
        match s with
        | Running -> "Running"
        | Stopped -> "Stopped"
        | QueueFault _ -> "Queue Fault"

/// Worker state object
[<NoEquality; NoComparison>]
type WorkerState =
    {
        /// Machine hostname
        Hostname : string
        /// Machine ProcessId
        ProcessId : int
        /// Machine 
        ProcessorCount : int
        /// Worker operation status
        Status : WorkerStatus
        /// Current number of executing jobs
        CurrentJobCount : int
        /// Maximum number of executing jobs
        MaxJobCount : int
    }

/// Worker state object
[<NoEquality; NoComparison>]
type WorkerInfo =
    {
        /// Worker reference unique identifier
        Id : IWorkerId
        /// Last Heartbeat submitted by worker
        LastHeartbeat : DateTime
        /// Heartbeat rate designated by worker manager
        HeartbeatRate : TimeSpan
        /// Time of worker initialization/subscription
        InitializationTime : DateTime
        /// Current worker state
        State : WorkerState
        /// Latest worker performance metrics
        PerformanceMetrics : NodePerformanceInfo
    }

/// Worker manager abstraction; must be serializable
type IWorkerManager =

    /// <summary>
    ///     Asynchronously returns current worker information
    ///     for provided worker reference.
    /// </summary>
    /// <param name="target">Worker ref to be examined.</param>
    abstract TryGetWorkerInfo : id:IWorkerId -> Async<WorkerInfo option>

    /// <summary>
    ///     Subscribe a new worker instance to the cluster.
    /// </summary>
    /// <param name="worker">Worker instance to be subscribed.</param>
    /// <param name="worker">Initial worker state.</param>
    /// <returns>Unsubscribe disposable.</returns>
    abstract SubscribeWorker : id:IWorkerId -> Async<IDisposable>

    /// <summary>
    ///     Asynchronously declares worker state for provided worker.
    /// </summary>
    /// <param name="worker">Worker id to be updated.</param>
    /// <param name="state">State for worker.</param>
    abstract DeclareWorkerState : id:IWorkerId * state:WorkerState -> Async<unit>

    /// <summary>
    ///     Asynchronously submits node performance metrics for provided worker.
    /// </summary>
    /// <param name="id">Worker id declaring performance metrics.</param>
    /// <param name="perf">Performance metrics for given worker.</param>
    abstract SubmitPerformanceMetrics : id:IWorkerId * perf:NodePerformanceInfo -> Async<unit>

    /// Asynchronously requests node performance metrics for all nodes.
    abstract GetAvailableWorkers : unit -> Async<WorkerInfo []>