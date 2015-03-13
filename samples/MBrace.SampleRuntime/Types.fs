module internal MBrace.SampleRuntime.Types

// Provides facility for the execution of cloud jobs.
// In this context, a job denotes a single work item to be sent
// to a worker node for execution. Jobs may span multiple threads
// but are bound to a single process. A cloud workflow that has
// been passed continuations is a typical example of such a job.

#nowarn "444"

open System

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace
open MBrace.Continuation
open MBrace.Store

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Serialization
open MBrace.Runtime.Vagabond
open MBrace.SampleRuntime.Actors

/// Information fixed to cloud process
type ProcessInfo =
    {
        /// Cloud process unique identifier
        ProcessId : string
        /// Cloud file store configuration
        FileStoreConfig : CloudFileStoreConfiguration
        /// Cloud atom configuration
        AtomConfig : CloudAtomConfiguration
        /// Cloud channel configuration
        ChannelConfig : CloudChannelConfiguration
    }

/// A job is a computation belonging to a larger cloud process
/// that is executed by a single worker machine.
type Job = 
    {
        /// Process info
        ProcessInfo : ProcessInfo
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
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    /// <summary>
    ///     Asynchronously executes job in the local process.
    /// </summary>
    /// <param name="runtimeProvider">Local scheduler implementation.</param>
    /// <param name="dependencies">Job dependent assemblies.</param>
    /// <param name="job">Job to be executed.</param>
    static member RunAsync (runtimeProvider : IDistributionProvider) (faultCount : int) (job : Job) = 
        async {
            let tem = new JobExecutionMonitor()
            let ctx =
                {
                    Resources = 
                        resource { 
                            yield runtimeProvider ; yield tem ; yield job.CancellationTokenSource ; 
                            yield Config.WithCachedFileStore job.ProcessInfo.FileStoreConfig
                            yield job.ProcessInfo.AtomConfig ; yield job.ProcessInfo.ChannelConfig
                        }

                    CancellationToken = job.CancellationTokenSource :> ICloudCancellationToken
                }

            if faultCount > 0 then
                // current job has already faulted once, 
                // consult user-provided fault policy for deciding how to proceed.
                let faultException = new FaultException(sprintf "Fault exception when running job '%s'." job.JobId)
                match job.FaultPolicy.Policy faultCount (faultException :> exn) with
                | None -> 
                    // fault policy decrees exception, pass fault to exception continuation
                    job.Econt ctx <| ExceptionDispatchInfo.Capture faultException
                | Some timeout ->
                    // fault policy decrees retry, sleep for specified time and execute
                    do! Async.Sleep (int timeout.TotalMilliseconds)
                    do job.StartJob ctx
            else
                // no faults, just execute the job
                do job.StartJob ctx

            return! JobExecutionMonitor.AwaitCompletion tem
        }


/// Pickled jobs can be dequeued from the job queue without
/// needing a priori Vagabond dependency loading
type PickledJob = 
    {
        /// Job identifier
        JobId : string
        /// Return type of given job
        TypeName : string
        /// Pickled job entry
        Pickle : Pickle<Job> 
        /// Assembly dependencies required for unpickling the job
        Dependencies : AssemblyId [] 
        /// Target worker for provided job
        Target : IWorkerRef option
    }
with
    /// <summary>
    ///     Create a pickled job out of given cloud workflow and continuations
    /// </summary>
    /// <param name="dependencies">Vagabond dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Cloud</param>
    static member Create procInfo dependencies cts fp sc ec cc worker (wf : Cloud<'T>) : PickledJob =
        let jobId = System.Guid.NewGuid().ToString()
        let runJob ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)

        let job = 
            { 
                Type = typeof<'T>
                ProcessInfo = procInfo
                JobId = jobId
                StartJob = runJob
                FaultPolicy = fp
                Econt = ec
                CancellationTokenSource = cts
            }

        let jobP = Config.Pickler.PickleTyped job

        { 
            JobId = jobId ;
            TypeName = Type.prettyPrint typeof<'T> ;
            Pickle = jobP ; 
            Dependencies = dependencies ; 
            Target = worker ;
        }

/// Defines a handle to the state of a runtime instance
/// All information pertaining to the runtime execution state
/// is contained in a single process -- the initializing client.
type RuntimeState =
    {
        /// TCP endpoint used by the runtime state container
        IPEndPoint : System.Net.IPEndPoint
        /// Reference to the global job queue employed by the runtime
        JobQueue : Queue<PickledJob, IWorkerRef>
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
        // job dequeue predicate -- checks if job is assigned to particular target
        let shouldDequeue (dequeueingWorker : IWorkerRef) (pt : PickledJob) =
            match pt.Target with
            // job not applicable to specific worker, approve dequeue
            | None -> true
            | Some w ->
                // job applicable to current worker, approve dequeue
                if w = dequeueingWorker then true
                else
                    // worker not applicable to current worker, dequeue if target worker has been disposed
                    getWorkers () |> Array.forall ((<>) dequeueingWorker)

        {
            IPEndPoint = MBrace.SampleRuntime.Config.LocalEndPoint
            Workers = Cell.Init getWorkers
            Logger = Logger.Init logger
            JobQueue = Queue<_,_>.Init shouldDequeue
            AssemblyExporter = AssemblyExporter.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    /// <summary>
    ///     Create a pickled job out of given cloud workflow and continuations
    /// </summary>
    /// <param name="dependencies">Vagabond dependency manifest.</param>
    /// <param name="cts">Distributed cancellation token source.</param>
    /// <param name="sc">Success continuation</param>
    /// <param name="ec">Exception continuation</param>
    /// <param name="cc">Cancellation continuation</param>
    /// <param name="wf">Cloud</param>
    member rt.EnqueueJob procInfo dependencies cts fp sc ec cc worker (wf : Cloud<'T>) : unit =
        rt.JobQueue.Enqueue <| PickledJob.Create procInfo dependencies cts fp sc ec cc worker wf

    /// <summary>
    ///     Atomically schedule a collection of tasks
    /// </summary>
    /// <param name="jobs">Jobs to be enqueued</param>
    member rt.EnqueueJobs jobs = rt.JobQueue.EnqueueMultiple jobs

    /// <summary>
    ///     Schedules a cloud workflow as a distributed task.
    /// </summary>
    /// <param name="dependencies">Declared workflow dependencies.</param>
    /// <param name="cts">Cancellation token source bound to job.</param>
    /// <param name="wf">Input workflow.</param>
    member rt.StartAsTask procInfo dependencies cts fp worker (wf : Cloud<'T>) : Async<ICloudTask<'T>> = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                JobExecutionMonitor.TriggerCompletion ctx
            } |> JobExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueJob procInfo dependencies cts fp scont econt ccont worker wf
        return resultCell :> ICloudTask<'T>
    }

    /// <summary>
    ///     Load Vagrant dependencies and unpickle job.
    /// </summary>
    /// <param name="pJob">Job to unpickle.</param>
    member rt.UnPickle(pJob : PickledJob) = async {
        do! rt.AssemblyExporter.LoadDependencies (Array.toList pJob.Dependencies)
        return Config.Pickler.UnPickleTyped pJob.Pickle
    }