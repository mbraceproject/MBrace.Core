namespace MBrace.Runtime

open System

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

open MBrace.Runtime.Utils

#nowarn "444"

/// Defines a serializable cancellation entry with global visibility that can
/// be cancelled or polled for cancellation.
type ICancellationEntry =
    inherit IAsyncDisposable
    /// Unique identifier for cancellation entry instance.
    abstract UUID : string
    /// Asynchronously checks if entry has been cancelled.
    abstract IsCancellationRequested : Async<bool>
    /// Asynchronously cancels the entry.
    abstract Cancel : unit -> Async<unit>

/// Defines a serializable cancellation entry manager with global visibility.
type ICancellationEntryFactory =
    /// Asynchronously creates a cancellation entry with no parents.
    abstract CreateCancellationEntry : unit -> Async<ICancellationEntry>

    /// <summary>
    ///     Asynchronously creates a cancellation entry provided parents. 
    ///     Returns 'None' if all parents have been cancelled.
    /// </summary>
    /// <param name="parents">Cancellation entry parents.</param>
    abstract TryCreateLinkedCancellationEntry : parents:ICancellationEntry[] -> Async<ICancellationEntry option>

/// Defines a distributed, atomic counter.
type ICloudCounter =
    inherit IAsyncDisposable
    /// Asynchronously increments the counter by 1.
    abstract Increment : unit -> Async<int>
    /// Asynchronously decrements the counter by 1.
    abstract Decrement : unit -> Async<int>
    /// Asynchronously fetches the current value for the counter.
    abstract Value     : Async<int>

/// Defines a distributed result aggregator.
type IResultAggregator<'T> =
    inherit IAsyncDisposable
    /// Declared capacity for result aggregator.
    abstract Capacity : int
    /// Asynchronously returns current accumulated size for aggregator.
    abstract CurrentSize : Async<int>
    /// Asynchronously returns if result aggregator has been completed.
    abstract IsCompleted : Async<bool>
    /// <summary>
    ///     Asynchronously sets result at given index to aggregator.
    ///     Returns true if aggregation is completed.
    /// </summary>
    /// <param name="index">Index to set value at.</param>
    /// <param name="value">Value to be set.</param>
    /// <param name="overwrite">Overwrite if value already exists at index.</param>
    abstract SetResult : index:int * value:'T * overwrite:bool -> Async<bool>
    /// <summary>
    ///     Asynchronously returns aggregated results from aggregator.
    /// </summary>
    abstract ToArray : unit -> Async<'T []>


/// Defines a distributed task completion source.
type ICloudTaskCompletionSource =
    inherit IAsyncDisposable
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

/// Defines a distributed task completion source.
type ICloudTaskCompletionSource<'T> =
    inherit ICloudTaskCompletionSource
    /// Gets the underlying cloud task for the task completion source.
    abstract Task : ICloudTask<'T>
    /// Asynchronously sets a completed result for the task.
    abstract SetCompleted : 'T -> Async<unit>



/// A cloud job is fragment of a cloud process to be executed in a single machine.
[<NoEquality; NoComparison>]
type CloudJob = 
    {
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Process info
        ProcessId : string
        /// Job unique identifier
        JobId : string
        /// Job type
        Type : Type
        /// Triggers job execution with worker-provided execution context
        StartJob : ExecutionContext -> unit
        /// Job fault policy
        FaultPolicy : FaultPolicy
        /// Exception continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to job
        CancellationToken : ICloudCancellationToken
        /// Parent task completion source; used for emergency fault declarations.
        ParentTask : ICloudTaskCompletionSource
    }
with

    /// <summary>
    ///     Create a cloud job out of given cloud workflow and continuations.
    /// </summary>
    /// <param name="dependencies">Vagabond dependencies for task.</param>
    /// <param name="processId">Process unque identifier.</param>
    /// <param name="parentTask">Cloud task completion source of current job.</param>
    /// <param name="token">Cancellation token for job.</param>
    /// <param name="faultPolicy">Fault policy for job.</param>
    /// <param name="scont">Success continuation.</param>
    /// <param name="econt">Exception continuation.</param>
    /// <param name="ccont">Cancellation continuation.</param>
    /// <param name="workflow">Workflow to be executed in job.</param>
    static member Create (dependencies : AssemblyId[], processId : string, parentTask : ICloudTaskCompletionSource, 
                            token : ICloudCancellationToken, faultPolicy : FaultPolicy, 
                            scont : ExecutionContext -> 'T -> unit, 
                            econt : ExecutionContext -> ExceptionDispatchInfo -> unit, 
                            ccont : ExecutionContext -> OperationCanceledException -> unit, 
                            workflow : Cloud<'T>) =

        let jobId = mkUUID()
        let runJob ctx =
            let cont = { Success = scont ; Exception = econt ; Cancellation = ccont }
            Cloud.StartWithContinuations(workflow, cont, ctx)

        { 
            ProcessId = processId
            Dependencies = dependencies
            JobId = jobId
            Type = typeof<'T>
            StartJob = runJob
            FaultPolicy = faultPolicy
            Econt = econt
            CancellationToken = token
            ParentTask = parentTask
        }

/// Cloud job lease token given to workers that dequeue it
type ICloudJobLeaseToken =
    /// Process identifier
    abstract ProcessId : string
    /// Job identifier
    abstract JobId : string
    /// Parent task completion source
    abstract ParentTask : ICloudTaskCompletionSource
    /// Get number of faults generated by this job together with
    /// the latest exception raised.
    abstract FaultState  : (int * exn) option
    /// Asynchronously fetches the actual job instance.
    abstract GetJob           : unit -> Async<CloudJob>
    /// Vagabond dependencies for job.
    abstract Dependencies     : AssemblyId []
    /// Asynchronously declare job to be completed.
    abstract DeclareCompleted : unit -> Async<unit>
    /// Asynchronously declare job to be faulted.
    abstract DeclareFaulted   : ExceptionDispatchInfo -> Async<unit>

/// Defines a distributed queue for jobs
type IJobQueue =
    /// <summary>
    ///     Asynchronously enqueue a singular job to queue.
    /// </summary>
    /// <param name="job">Job to be enqueued.</param>
    /// <param name="target">Optional target worker.</param>
    abstract Enqueue : job:CloudJob * ?target:IWorkerRef -> Async<unit>

    /// <summary>
    ///     Asynchronoulsy enqueue a batch of jobs to queue.
    /// </summary>
    /// <param name="jobs">Jobs to be enqueued, accompanied by optional target worker.</param>
    abstract BatchEnqueue : jobs:(CloudJob * IWorkerRef option) [] -> Async<unit>

    /// <summary>
    ///     Asynchronously attempt to dequeue a job, if it exists.
    /// </summary>
    /// <param name="id">WorkerRef identifying current worker.</param>
    abstract TryDequeue : id:IWorkerRef -> Async<ICloudJobLeaseToken option>

/// Defines a Vagabond assembly manager
type IAssemblyManager =
    /// <summary>
    ///     Uploads assemblies and data dependencies to runtime.
    /// </summary>
    /// <param name="assemblies">Local Vagabond assembly descriptors to be uploaded.</param>
    abstract UploadAssemblies : assemblies:seq<VagabondAssembly> -> Async<unit>

    /// <summary>
    ///     Downloads assemblies and data dependencies from runtime.
    /// </summary>
    /// <param name="ids">Assembly identifiers to be downloaded.</param>
    abstract DownloadAssemblies : ids:seq<AssemblyId> -> Async<VagabondAssembly []>

    /// <summary>
    ///     Loads provided vagabond assemblies to current application domain.
    /// </summary>
    /// <param name="assemblies">Assemblies to be loaded.</param>
    abstract LoadAssemblies : assemblies:seq<VagabondAssembly> -> AssemblyLoadInfo []

    /// <summary>
    ///     Computes vagabond dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be computed.</param>
    abstract ComputeDependencies : graph:obj -> VagabondAssembly []

    /// <summary>
    ///     Registers a native dependency for instance.
    /// </summary>
    /// <param name="path"></param>
    abstract RegisterNativeDependency : path:string -> VagabondAssembly

    /// Gets native dependencies for assembly
    abstract NativeDependencies : VagabondAssembly []

/// Defines an abstract runtime resource provider. Should not be serializable
type IRuntimeResourceManager =
    /// Local MBrace resources to be supplied to workflow.
    abstract ResourceRegistry : ResourceRegistry
    /// Asynchronously returns all workers in the current cluster.
    abstract GetAvailableWorkers : unit -> Async<IWorkerRef []>
    /// Gets the WorkerRef identifying the current worker instance.
    abstract CurrentWorker : IWorkerRef
    /// Logger abstraction used for cloud workflows.
    abstract Logger : ICloudLogger
    /// Serializable distributed cancellation entry manager.
    abstract CancellationEntryFactory : ICancellationEntryFactory
    /// Gets the job queue instance for the runtime.
    abstract JobQueue : IJobQueue
    /// Gets the Vagabond assembly manager for this runtime.
    abstract AssemblyManager : IAssemblyManager

    /// <summary>
    ///     Asynchronously creates a new distributed counter with supplied initial value.
    /// </summary>
    /// <param name="initialValue">Supplied initial value.</param>
    abstract RequestCounter : initialValue:int -> Async<ICloudCounter>

    /// <summary>
    ///     Asynchronously creates a new result aggregator with supplied capacity.
    /// </summary>
    /// <param name="capacity">Number of items to be aggregated.</param>
    abstract RequestResultAggregator<'T> : capacity:int -> Async<IResultAggregator<'T>>

    /// Asynchronously creates a new distributed task completion source.
    abstract RequestTaskCompletionSource<'T> : unit -> Async<ICloudTaskCompletionSource<'T>>