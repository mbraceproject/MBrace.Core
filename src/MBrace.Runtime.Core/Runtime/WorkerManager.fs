﻿namespace MBrace.Runtime

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
        /// Worker reference identifier
        WorkerRef : IWorkerRef
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

/// Worker manager abstraction
type IWorkerManager =

    /// <summary>
    /// Returns true if supplied worker implementation
    /// is valid and belonging to this cluster.
    /// </summary>
    /// <param name="target">worker ref to be examined.</param>
    abstract IsValidTargetWorker : target:IWorkerRef -> Async<bool>

    /// <summary>
    ///     Asynchronously returns current worker information
    ///     for provided worker reference.
    /// </summary>
    /// <param name="target">Worker ref to be examined.</param>
    abstract TryGetWorkerInfo : target:IWorkerRef -> Async<WorkerInfo option>

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
    abstract GetAvailableWorkers : unit -> Async<WorkerInfo []>