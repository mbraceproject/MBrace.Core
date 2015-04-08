namespace MBrace.Runtime

// Distributed distribution provider definition
// Cloud workflows actuating parallelism should
// be passed an instance of ICloudRuntimeProvider in their
// ExecutionContext.

open MBrace.Core

/// <summary>
///     Executing runtime abstraction.
/// </summary>
type IDistributionProvider =

    /// Get cloud process identifier
    abstract ProcessId : string
    /// Get cloud job identifier
    abstract JobId : string
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
    ///     Creates a linked cancellation token source given collection of cloud cancellation tokens.
    /// </summary>
    /// <param name="parents">Parent cancellation tokens.</param>
    abstract CreateLinkedCancellationTokenSource : parents:ICloudCancellationToken[] -> Async<ICloudCancellationTokenSource>

    /// <summary>
    ///     Creates a new scheduler instance with updated fault policy.
    /// </summary>
    /// <param name="newPolicy">new fault policy.</param>
    abstract WithFaultPolicy : newPolicy:FaultPolicy -> IDistributionProvider

    /// Specifies whether runtime supports submission of tasks to specific worker nodes
    abstract IsTargetedWorkerSupported : bool

    /// Specifies whether runtime will evaluate all parallelism primitives using threadpool semantics 
    abstract IsForcedLocalParallelismEnabled : bool

    /// <summary>
    ///     Creates a new distribution provider instance with toggled local parallelism semantics
    /// </summary>
    /// <param name="localParallelismEnabled">With local parallelism enabled or disabled.</param>
    abstract WithForcedLocalParallelismSetting : localParallelismEnabled:bool -> IDistributionProvider

    /// <summary>
    ///     Parallel fork/join implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleParallel : computations:seq<#Cloud<'T> * IWorkerRef option> -> Cloud<'T []>

    /// <summary>
    ///     Parallel nondeterministic choice implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleChoice : computations:seq<#Cloud<'T option> * IWorkerRef option> -> Cloud<'T option>

    /// <summary>
    ///     Parallel fork/join implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleLocalParallel : computations:seq<Local<'T>> -> Local<'T []>

    /// <summary>
    ///     Parallel nondeterministic choice implementation.
    /// </summary>
    /// <param name="computations">Computations to be executed. Contains optional target worker.</param>
    abstract ScheduleLocalChoice : computations:seq<Local<'T option>> -> Local<'T option>

    /// <summary>
    ///     Start a new computation as a cloud task. 
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="faultPolicy">Fault policy for new task.</param>
    /// <param name="cancellationToken">Cancellation token for task. Defaults to no cancellation token.</param>
    /// <param name="target">Explicitly specify a target worker for execution.</param>
    abstract ScheduleStartAsTask : workflow:Cloud<'T> * faultPolicy:FaultPolicy * ?cancellationToken:ICloudCancellationToken * ?target:IWorkerRef -> Cloud<ICloudTask<'T>>