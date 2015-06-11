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
    | ParallelChild     = 2
    /// Job associated with the child computation of a Choice workflow.
    | ChoiceChild       = 3


/// Fault metadata of provided cloud job
[<NoEquality; NoComparison>]
type JobFaultInfo =
    /// No faults associated with specified job
    | NoFault
    /// Faults have been declared by worker while processing job
    | FaultDeclaredByWorker of faultCount:int * latestException:ExceptionDispatchInfo * worker:IWorkerRef
    /// Worker has died while processing job
    | WorkerDeathWhileProcessingJob of faultCount:int * worker:IWorkerRef
    /// Job salvaged from targeted queue of a dead worker
    | IsTargetedJobOfDeadWorker of faultCount:int * worker:IWorkerRef
with
    /// Number of faults that occurred with current job.
    member jfi.FaultCount =
        match jfi with
        | NoFault -> 0
        | FaultDeclaredByWorker (fc,_,_) -> fc
        | WorkerDeathWhileProcessingJob (fc,_) -> fc
        | IsTargetedJobOfDeadWorker (fc,_) -> fc

/// Cloud task information
[<NoEquality; NoComparison>]
type CloudTaskInfo =
    {
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Cloud task unique identifier
        TaskId : string
        /// User-specified task name
        Name : string option
        /// Task return type
        Type : string
        /// Task cancellation object; used for emergency fault declarations.
        Canceller : ICloudTaskCanceller
    }

/// A cloud job is fragment of a cloud process to be executed in a single machine.
[<NoEquality; NoComparison>]
type CloudJob = 
    {
        /// Task information
        TaskInfo : CloudTaskInfo
        /// Cloud Job unique identifier
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
        /// Job Exception continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to job
        CancellationToken : ICloudCancellationToken
    }
with

    /// <summary>
    ///     Create a cloud job out of given cloud workflow and continuations.
    /// </summary>
    /// <param name="taskInfo">Parent task info.</param>
    /// <param name="token">Cancellation token for job.</param>
    /// <param name="faultPolicy">Fault policy for job.</param>
    /// <param name="scont">Success continuation.</param>
    /// <param name="econt">Exception continuation.</param>
    /// <param name="ccont">Cancellation continuation.</param>
    /// <param name="workflow">Workflow to be executed in job.</param>
    /// <param name="target">Declared target worker reference for computation to be executed.</param>
    static member Create (taskInfo : CloudTaskInfo, token : ICloudCancellationToken, faultPolicy : FaultPolicy, 
                            scont : ExecutionContext -> 'T -> unit, 
                            econt : ExecutionContext -> ExceptionDispatchInfo -> unit, 
                            ccont : ExecutionContext -> OperationCanceledException -> unit,
                            jobType : JobType, workflow : Cloud<'T>, ?target : IWorkerRef) =

        let jobId = mkUUID()
        let runJob ctx =
            let cont = { Success = scont ; Exception = econt ; Cancellation = ccont }
            Cloud.StartWithContinuations(workflow, cont, ctx)

        {
            TaskInfo = taskInfo
            JobId = jobId
            Type = typeof<'T>
            JobType = jobType
            StartJob = runJob
            FaultPolicy = faultPolicy
            Econt = econt
            CancellationToken = token
            TargetWorker = target
        }

/// Cloud job lease token given to workers that dequeue it
type ICloudJobLeaseToken =
    /// Task identifier
    abstract TaskId : string
    /// Job identifier
    abstract JobId : string
    /// Declared target worker for job
    abstract TargetWorker : IWorkerRef option
    /// String identifier of workflow return type.
    abstract Type : string
    /// Job creation metadata
    abstract JobType : JobType
    /// Parent cloud task info
    abstract TaskInfo : CloudTaskInfo
    /// Gets fault metadata associated with this job instance.
    abstract FaultInfo  : JobFaultInfo
    /// Asynchronously fetches the actual job instance.
    abstract GetJob           : unit -> Async<CloudJob>
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
    abstract BatchEnqueue : jobs:CloudJob[] -> Async<unit>

    /// <summary>
    ///     Asynchronously attempt to dequeue a job, if it exists.
    /// </summary>
    /// <param name="id">WorkerRef identifying current worker.</param>
    abstract TryDequeue : id:IWorkerRef -> Async<ICloudJobLeaseToken option>