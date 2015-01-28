module internal MBrace.SampleRuntime.Tasks

// Provides facility for the execution of tasks.
// In this context, a task denotes a single work item to be sent
// to a worker node for execution. Tasks may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a task.

open System
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Serialization
open MBrace.Runtime.Vagabond
open MBrace.SampleRuntime.Actors

// Tasks are cloud workflows that have been attached to continuations.
// In that sense they are 'closed' multi-threaded computations that
// are difficult to reason about from a worker node's point of view.
// TaskExecutionMonitor provides a way to cooperatively track execution
// of such 'closed' computations.

/// Provides a mechanism for cooperative task execution monitoring.
[<AutoSerializable(false)>]
type TaskExecutionMonitor () =
    let tcs = TaskCompletionSource<unit> ()
    static let fromContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskExecutionMonitor> ()

    member __.Task = tcs.Task
    member __.TriggerFault (e : exn) = tcs.TrySetException e |> ignore
    member __.TriggerCompletion () = tcs.TrySetResult () |> ignore

    /// Runs a single threaded, synchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectSync ctx (f : unit -> unit) : unit =
        let tem = fromContext ctx
        try f () with e -> tem.TriggerFault e |> ignore

    /// Runs an asynchronous computation,
    /// triggering the contextual TaskExecutionMonitor on uncaught exception
    static member ProtectAsync ctx (f : Async<unit>) : unit =
        let tem = fromContext ctx
        Async.StartWithContinuations(f, ignore, tem.TriggerFault, ignore)   

    /// Triggers task completion on the contextual TaskExecutionMonitor
    static member TriggerCompletion ctx =
        let tem = fromContext ctx in tem.TriggerCompletion () |> ignore

    /// Triggers task fault on the contextual TaskExecutionMonitor
    static member TriggerFault (ctx, e) =
        let tem = fromContext ctx in tem.TriggerFault e |> ignore

    /// Asynchronously await completion of provided TaskExecutionMonitor
    static member AwaitCompletion (tem : TaskExecutionMonitor) = async {
        try
            return! Async.AwaitTask tem.Task
        with :? System.AggregateException as e when e.InnerException <> null ->
            return! Async.Raise e.InnerException
    }

/// Process information record
type ProcessInfo =
    {
        /// Cloud process unique identifier
        ProcessId : string
        /// Default file store container for process
        DefaultDirectory : string
        /// Default atom container for process
        DefaultAtomContainer : string
        /// Default channel container for process
        DefaultChannelContainer : string
    }

/// Defines a task to be executed in a worker node
type Task = 
    {
        /// Return type of the defining cloud workflow.
        Type : Type
        /// Cloud process information
        ProcessInfo : ProcessInfo
        /// Task unique identifier
        TaskId : string
        /// Triggers task execution with worker-provided execution context
        StartTask : ExecutionContext -> unit
        /// Task fault policy
        FaultPolicy : FaultPolicy
        /// Exception Continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to task
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    /// <summary>
    ///     Asynchronously executes task in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Task dependent assemblies.</param>
    /// <param name="task">Task to be executed.</param>
    static member RunAsync (runtimeProvider : ICloudRuntimeProvider)
                            (atomProvider : ICloudAtomProvider)
                            (channelProvider : ICloudChannelProvider) 
                            (dependencies : AssemblyId list) (faultCount : int)
                            (task : Task) = 
        async {
            let tem = new TaskExecutionMonitor()
            let ctx =
                {
                    Resources = 
                        resource { 
                            yield runtimeProvider ; yield tem ; yield task.CancellationTokenSource ; 
                            yield Config.getFileStoreConfiguration task.ProcessInfo.DefaultDirectory ;
                            yield { AtomProvider = atomProvider ; DefaultContainer = task.ProcessInfo.DefaultAtomContainer } ;
                            yield { ChannelProvider = channelProvider ; DefaultContainer = task.ProcessInfo.DefaultChannelContainer } ;
                            yield channelProvider ; yield dependencies 
                        }

                    CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
                }

            if faultCount > 0 then
                // current task has already faulted once, 
                // consult user-provided fault policy for deciding how to proceed.
                let faultException = new FaultException(sprintf "Fault exception when running task '%s'." task.TaskId)
                match task.FaultPolicy.Policy faultCount (faultException :> exn) with
                | None -> 
                    // fault policy decrees exception, pass fault to exception continuation
                    task.Econt ctx <| ExceptionDispatchInfo.Capture faultException
                | Some timeout ->
                    // fault policy decrees retry, sleep for specified time and execute
                    do! Async.Sleep (int timeout.TotalMilliseconds)
                    do task.StartTask ctx
            else
                // no faults, just execute the task
                do task.StartTask ctx

            return! TaskExecutionMonitor.AwaitCompletion tem
        }


/// Type of pickled task as represented in the task queue
type PickledTask = 
    {
        Task : Pickle<Task> 
        Dependencies : AssemblyId list 
        Target : IWorkerRef option
    }
with
    /// <summary>
    ///     Create a pickled task out of given cloud workflow and continuations
    /// </summary>
    /// <param name="dependencies">Vagabond dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Workflow</param>
    static member CreateTask procInfo dependencies cts fp sc ec cc worker (wf : Cloud<'T>) : PickledTask =
        let taskId = System.Guid.NewGuid().ToString()
        let startTask ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)

        let task = 
            { 
                Type = typeof<'T>
                ProcessInfo = procInfo
                TaskId = taskId
                StartTask = startTask
                FaultPolicy = fp
                Econt = ec
                CancellationTokenSource = cts
            }

        let taskp = VagabondRegistry.Pickler.PickleTyped task

        { Task = taskp ; Dependencies = dependencies ; Target = worker }

/// Defines a handle to the state of a runtime instance
/// All information pertaining to the runtime execution state
/// is contained in a single process -- the initializing client.
type RuntimeState =
    {
        /// TCP endpoint used by the runtime state container
        IPEndPoint : System.Net.IPEndPoint
        /// Reference to the global task queue employed by the runtime
        /// Queue contains pickled task and its vagabond dependency manifest
        TaskQueue : Queue<PickledTask, IWorkerRef>
        /// Reference to a Vagabond assembly exporting actor.
        AssemblyExporter : AssemblyExporter
        /// Reference to the runtime resource manager
        /// Used for generating latches, cancellation tokens and result cells.
        ResourceFactory : ResourceFactory
        /// returns a manifest of workers available to the cluster.
        Workers : Cell<IWorkerRef []>
        /// Distributed logger facility
        Logger : Logger
    }
with
    /// Initialize a new runtime state in the local process
    static member InitLocal (logger : string -> unit) (getWorkers : unit -> IWorkerRef []) =
        // task dequeue predicate -- checks if task is assigned to particular target
        let shouldDequeue (dequeueingWorker : IWorkerRef) (pt : PickledTask) =
            match pt.Target with
            // task not applicable to specific worker, approve dequeue
            | None -> true
            | Some w ->
                // task applicable to current worker, approve dequeue
                if w = dequeueingWorker then true
                else
                    // worker not applicable to current worker, dequeue if target worker has been disposed
                    getWorkers () |> Array.forall ((<>) dequeueingWorker)

        {
            IPEndPoint = MBrace.SampleRuntime.Config.getLocalEndpoint()
            Workers = Cell.Init getWorkers
            Logger = Logger.Init logger
            TaskQueue = Queue<_,_>.Init shouldDequeue
            AssemblyExporter = AssemblyExporter.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    /// <summary>
    ///     Create a pickled task out of given cloud workflow and continuations
    /// </summary>
    /// <param name="dependencies">Vagabond dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Workflow</param>
    member rt.EnqueueTask procInfo dependencies cts fp sc ec cc worker (wf : Cloud<'T>) : unit =
        rt.TaskQueue.Enqueue <| PickledTask.CreateTask procInfo dependencies cts fp sc ec cc worker wf

    /// <summary>
    ///     Atomically schedule a collection of tasks
    /// </summary>
    /// <param name="tasks">Tasks to be enqueued</param>
    member rt.EnqueueTasks tasks = rt.TaskQueue.EnqueueMultiple tasks

    /// <summary>
    ///     Schedules a cloud workflow as a distributed result cell.
    ///     Used for root-level workflows or child tasks.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to task.</param>
    /// <param name="wf">Input workflow.</param>
    member rt.StartAsCell procInfo dependencies cts fp worker (wf : Cloud<'T>) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                TaskExecutionMonitor.TriggerCompletion ctx
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueTask procInfo dependencies cts fp scont econt ccont worker wf
        return resultCell
    }

    /// Attempt to dequeue a task from the runtime task queue
    member rt.TryDequeue (dequeueingWorker : IWorkerRef) = async {
        let! item =  rt.TaskQueue.TryDequeue dequeueingWorker
        match item with
        | None -> return None
        | Some (pt, faultCount, leaseMonitor) -> 
            do! rt.AssemblyExporter.LoadDependencies pt.Dependencies
            let task = VagabondRegistry.Pickler.UnPickleTyped pt.Task
            return Some (task, pt.Dependencies, faultCount, leaseMonitor)
    }