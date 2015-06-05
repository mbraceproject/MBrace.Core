namespace MBrace.Runtime

open System

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Cloud Job creation metadata
type JobType =
    /// Job associated with a root task creation.
    | TaskRoot          = 1
    /// Job associated with the child computation of a Parallel workflow.
    | ChildParallel     = 2
    /// Job associated with the child computation of a Choice workflow.
    | ChildChoice       = 3


/// Fault metadata of provided cloud job
[<NoEquality; NoComparison>]
type JobFaultInfo =
    /// No faults associated with specified job
    | NoFault
    /// Faults have been declared by worker while processing job
    | FaultDeclaredByWorker of faultCount:int * latestException:ExceptionDispatchInfo
    /// Worker has died while processing job
    | WorkerDeathWhileProcessingJob of faultCount:int * latestWorker:IWorkerRef
    /// Job salvaged from targeted queue of a dead worker
    | IsTargetedJobOfDeadWorker

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
        /// Job workflow 'return type'
        Type : Type
        /// Job creation metadata
        JobType : JobType
        /// Declared target worker for job
        TargetWorker : IWorkerRef option
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
    /// <param name="target">Declared target worker reference for computation to be executed.</param>
    static member Create (dependencies : AssemblyId[], processId : string, parentTask : ICloudTaskCompletionSource, 
                            token : ICloudCancellationToken, faultPolicy : FaultPolicy, 
                            scont : ExecutionContext -> 'T -> unit, 
                            econt : ExecutionContext -> ExceptionDispatchInfo -> unit, 
                            ccont : ExecutionContext -> OperationCanceledException -> unit,
                            jobType : JobType, workflow : Cloud<'T>, ?target : IWorkerRef) =

        let jobId = mkUUID()
        let runJob ctx =
            let cont = { Success = scont ; Exception = econt ; Cancellation = ccont }
            Cloud.StartWithContinuations(workflow, cont, ctx)

        { 
            ProcessId = processId
            Dependencies = dependencies
            JobId = jobId
            Type = typeof<'T>
            JobType = jobType
            StartJob = runJob
            FaultPolicy = faultPolicy
            Econt = econt
            CancellationToken = token
            ParentTask = parentTask
            TargetWorker = target
        }

/// Cloud job lease token given to workers that dequeue it
type ICloudJobLeaseToken =
    /// Process identifier
    abstract ProcessId : string
    /// Job identifier
    abstract JobId : string
    /// Declared target worker for job
    abstract TargetWorker : IWorkerRef option
    /// String identifier of workflow return type.
    abstract Type : string
    /// Job creation metadata
    abstract JobType : JobType
    /// Parent task completion source
    abstract ParentTask : ICloudTaskCompletionSource
    /// Gets fault metadata associated with this job instance.
    abstract FaultInfo  : JobFaultInfo
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
    abstract Enqueue : job:CloudJob -> Async<unit>

    /// <summary>
    ///     Asynchronoulsy enqueue a batch of jobs to queue.
    /// </summary>
    /// <param name="jobs">Jobs to be enqueued.</param>
    abstract BatchEnqueue : jobs:CloudJob [] -> Async<unit>

    /// <summary>
    ///     Asynchronously attempt to dequeue a job, if it exists.
    /// </summary>
    /// <param name="id">WorkerRef identifying current worker.</param>
    abstract TryDequeue : id:IWorkerRef -> Async<ICloudJobLeaseToken option>