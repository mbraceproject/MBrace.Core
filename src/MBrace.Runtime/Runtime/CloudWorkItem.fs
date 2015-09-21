namespace MBrace.Runtime

open System

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Unique cloud work item identifier
type CloudWorkItemId = Guid

/// Cloud work item creation metadata
type CloudWorkItemType =
    /// Work item associated with a root cloud process creation.
    | ProcessRoot
    /// Work item associated with the child computation of a Parallel workflow.
    | ParallelChild of index:int * size:int
    /// Work item associated with the child computation of a Choice workflow.
    | ChoiceChild of index:int * size:int


/// Fault metadata of provided cloud work item
[<NoEquality; NoComparison>]
type CloudWorkItemFaultInfo =
    /// No faults associated with specified work item
    | NoFault
    /// Faults have been declared by worker while processing work item
    | FaultDeclaredByWorker of faultCount:int * latestException:ExceptionDispatchInfo * worker:IWorkerId
    /// Worker has died while processing work item
    | WorkerDeathWhileProcessingWorkItem of faultCount:int * worker:IWorkerId
    /// Work item salvaged from targeted queue of a dead worker
    | IsTargetedWorkItemOfDeadWorker of faultCount:int * worker:IWorkerId
    /// Number of faults that occurred with current work item.
    member jfi.FaultCount =
        match jfi with
        | NoFault -> 0
        | FaultDeclaredByWorker (fc,_,_) -> fc
        | WorkerDeathWhileProcessingWorkItem (fc,_) -> fc
        | IsTargetedWorkItemOfDeadWorker (fc,_) -> fc
        

/// A cloud work item is fragment of a cloud process to be executed in a single machine.
[<NoEquality; NoComparison>]
type CloudWorkItem = 
    {
        /// Cloud work item unique identifier
        Id : CloudWorkItemId
        /// Work item workflow 'return type';
        /// Work items have no return type per se but this indicates the return type of 
        /// the initial computation that is being passed to its continuations.
        Type : Type
        /// Parent cloud process entry for work item
        Process : ICloudProcessEntry
        /// Work item creation metadata
        WorkItemType : CloudWorkItemType
        /// Declared target worker for work item
        TargetWorker : IWorkerId option
        /// Triggers work item execution with worker-provided execution context
        StartWorkItem : ExecutionContext -> unit
        /// Work item fault policy
        FaultPolicy : IFaultPolicy
        /// Work item Fault continuation
        FaultCont : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to work item
        CancellationToken : ICloudCancellationToken
    }
    /// <summary>
    ///     Create a cloud work item out of given cloud workflow and continuations.
    /// </summary>
    /// <param name="procEntry">Parent cloud process entry.</param>
    /// <param name="token">Cancellation token for work item.</param>
    /// <param name="faultPolicy">Fault policy for work item.</param>
    /// <param name="scont">Success continuation.</param>
    /// <param name="econt">Exception continuation.</param>
    /// <param name="ccont">Cancellation continuation.</param>
    /// <param name="workflow">Workflow to be executed in work item.</param>
    /// <param name="target">Declared target worker reference for computation to be executed.</param>
    /// <param name="fcont">Fault continuation. Defaults to exception continuation if not specified.</param>
    static member Create (procEntry : ICloudProcessEntry, token : ICloudCancellationToken, faultPolicy : IFaultPolicy, 
                            scont : ExecutionContext -> 'T -> unit, 
                            econt : ExecutionContext -> ExceptionDispatchInfo -> unit,
                            ccont : ExecutionContext -> OperationCanceledException -> unit,
                            workItemType : CloudWorkItemType, workflow : Cloud<'T>, 
                            ?fcont : ExecutionContext -> ExceptionDispatchInfo -> unit,
                            ?target : IWorkerId) =

        let workItem = Guid.NewGuid()
        let runWorkItem ctx =
            let cont = { Success = scont ; Exception = econt ; Cancellation = ccont }
            Cloud.StartWithContinuations(workflow, cont, ctx)

        {
            Process = procEntry
            Id = workItem
            Type = typeof<'T>
            WorkItemType = workItemType
            StartWorkItem = runWorkItem
            FaultPolicy = faultPolicy
            FaultCont = defaultArg fcont econt
            CancellationToken = token
            TargetWorker = target
        }

/// Cloud work item lease token given to workers that dequeue it
type ICloudWorkItemLeaseToken =
    /// Parent cloud process info
    abstract Process : ICloudProcessEntry
    /// Cloud work item identifier
    abstract Id : CloudWorkItemId
    /// Declared target worker for work item
    abstract TargetWorker : IWorkerId option
    /// String identifier of workflow return type.
    abstract Type : string
    /// Work item creation metadata
    abstract WorkItemType : CloudWorkItemType
    /// Work item payload size in bytes
    abstract Size : int64
    /// Gets fault metadata associated with this work item.
    abstract FaultInfo  : CloudWorkItemFaultInfo
    /// Asynchronously fetches the actual work item.
    abstract GetWorkItem           : unit -> Async<CloudWorkItem>
    /// Asynchronously declare work item to be completed.
    abstract DeclareCompleted : unit -> Async<unit>
    /// Asynchronously declare work item to be faulted.
    abstract DeclareFaulted   : ExceptionDispatchInfo -> Async<unit>

/// Defines a distributed queue for work items
type ICloudWorkItemQueue =
    /// <summary>
    ///     Asynchronously enqueue a singular work item to queue.
    /// </summary>
    /// <param name="workItem">Work item to be enqueued.</param>
    /// <param name="isClientSideEnqueue">Declares that work item is being enqueued on the client side.</param>
    abstract Enqueue : workItem:CloudWorkItem * isClientSideEnqueue:bool -> Async<unit>

    /// <summary>
    ///     Asynchronoulsy enqueue a batch of work items to queue.
    /// </summary>
    /// <param name="workItems">Work items to be enqueued.</param>
    abstract BatchEnqueue : wormItems:CloudWorkItem[] -> Async<unit>

    /// <summary>
    ///     Asynchronously attempt to dequeue a work item, if it exists.
    /// </summary>
    /// <param name="id">WorkerRef identifying current worker.</param>
    abstract TryDequeue : id:IWorkerId -> Async<ICloudWorkItemLeaseToken option>
