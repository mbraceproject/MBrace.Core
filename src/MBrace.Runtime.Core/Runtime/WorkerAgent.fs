namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PerformanceMonitor

[<NoEquality; NoComparison>]
type private WorkerAgentMessage = 
    | GetStatus of ReplyChannel<WorkerJobExecutionStatus>
    | Stop of waitTimeout:int * ReplyChannel<unit>
    | Start of ReplyChannel<unit>

/// Worker agent with updatable configuration
type WorkerAgent private (runtime : IRuntimeManager, workerId : IWorkerId, jobEvaluator : ICloudJobEvaluator, maxConcurrentJobs : int, unsubscriber : IDisposable, submitPerformanceMetrics : bool) =
    let cts = new CancellationTokenSource()
    // TODO : keep a detailed record of jobs, possibly add provision for forcible termination.
    let mutable currentJobCount = 0

    let logger = runtime.SystemLogger
    let waitInterval = 100
    let errorInterval = 1000

    let rec workerLoop (status : WorkerJobExecutionStatus) (inbox : MailboxProcessor<WorkerAgentMessage>) = async {
        let! controlMessage = async {
            if inbox.CurrentQueueLength > 0 then
                let! m = inbox.Receive()
                return Some m
            else
                return None
        }

        match controlMessage with
        | None ->
            match status with
            | Stopped ->
                do! Async.Sleep waitInterval
                return! workerLoop status inbox

            | _ when currentJobCount >= maxConcurrentJobs ->
                do! Async.Sleep waitInterval
                return! workerLoop status inbox

            | _ ->
                let! job = Async.Catch <| runtime.JobQueue.TryDequeue workerId
                match job with
                | Choice2Of2 e ->
                    let status = QueueFault (ExceptionDispatchInfo.Capture e)
                    do! runtime.WorkerManager.DeclareWorkerStatus(workerId, status)
                    logger.Logf LogLevel.Info "Worker Job Queue fault:\n%O" e
                    do! Async.Sleep errorInterval
                    return! workerLoop status inbox

                | Choice1Of2 result ->
                    let! status = async {
                        match status with
                        | QueueFault _ ->
                            let status = WorkerJobExecutionStatus.Running
                            logger.Log LogLevel.Info "Worker Job Queue restored."
                            do! runtime.WorkerManager.DeclareWorkerStatus(workerId, status)
                            return status

                        | _ -> return status
                    }

                    match result with
                    | None ->
                        do! Async.Sleep waitInterval
                        return! workerLoop status inbox

                    | Some jobToken ->
                        // Successfully dequeued job, run it.
                        if jobToken.JobType = JobType.TaskRoot then
                            do! runtime.TaskManager.DeclareStatus(jobToken.TaskInfo.Id, Dequeued)

                        let jc = Interlocked.Increment &currentJobCount
                        do! runtime.WorkerManager.IncrementJobCount workerId

                        logger.Logf LogLevel.Info "Dequeued cloud job '%s'." jobToken.Id
                        logger.Logf LogLevel.Info "Concurrent job count increased to %d/%d." jc maxConcurrentJobs

                        let! _ = Async.StartChild <| async { 
                            try
                                try
                                    let! assemblies = runtime.AssemblyManager.DownloadAssemblies jobToken.TaskInfo.Dependencies
                                    do! jobEvaluator.Evaluate (assemblies, jobToken)
                                with e ->
                                    do! runtime.TaskManager.DeclareFaultedJob(jobToken.TaskInfo.Id)
                                    logger.Logf LogLevel.Error "Job '%s' faulted at initialization:\n%A" jobToken.Id e
                                    return ()
                            finally
                                let jc = Interlocked.Decrement &currentJobCount
                                runtime.WorkerManager.DecrementJobCount workerId |> Async.Catch |> Async.Ignore |> Async.Start
                                logger.Logf LogLevel.Info "Concurrent job count decreased to %d/%d." jc maxConcurrentJobs

                        }

                        do! Async.Sleep waitInterval
                        return! workerLoop status inbox

        | Some(Stop (waitTimeout, rc)) ->
            match status with
            | WorkerJobExecutionStatus.Running | QueueFault _ ->
                logger.Log LogLevel.Info "Stop requested. Waiting for pending jobs."
                let rec wait () = async {
                    let jc = currentJobCount
                    if jc > 0 then
                        logger.Logf LogLevel.Info "Waiting for (%d) active jobs to complete." jc
                        do! Async.Sleep 1000
                        return! wait ()
                }

                let! result = Async.WithTimeout(wait (), waitTimeout) |> Async.Catch
                match result with
                | Choice1Of2 () -> logger.Log LogLevel.Info "No active jobs."
                | Choice2Of2 _ -> logger.Logf LogLevel.Error "Timeout while waiting for active jobs to complete."

                let status = Stopped
                do! runtime.WorkerManager.DeclareWorkerStatus(workerId, status)
                do rc.Reply (())

                logger.Log LogLevel.Info "Worker stopped."
                return! workerLoop Stopped inbox

            | _ ->
                rc.ReplyWithError (new InvalidOperationException("Worker is not running."))
                return! workerLoop status inbox

        | Some(Start rc) ->
            match status with
            | Stopped ->
                logger.Log LogLevel.Info "Starting Worker."
                let status = WorkerJobExecutionStatus.Running
                do! runtime.WorkerManager.DeclareWorkerStatus (workerId, status)
                do rc.Reply (())
                return! workerLoop status inbox
            | _ ->
                rc.ReplyWithError (new InvalidOperationException "Worker is already running.")
                return! workerLoop status inbox

        | Some(GetStatus rc) ->
            rc.Reply status
            return! workerLoop status inbox
    }

    let agent = MailboxProcessor.Start(workerLoop Stopped, cts.Token)

    let perfmon =
        if submitPerformanceMetrics then
            let perfmon = new PerformanceMonitor()
            perfmon.Start()

            let rec perfLoop () = async {
                let perf = perfmon.GetCounters()
                try 
                    do! runtime.WorkerManager.SubmitPerformanceMetrics(workerId, perf)
                    do! Async.Sleep 1000
                with e -> 
                    runtime.SystemLogger.Logf LogLevel.Error "Error submitting performance metrics:\n%O" e
                    do! Async.Sleep 2000

                return! perfLoop ()
            }

            Async.Start(perfLoop(), cts.Token)
            Some perfmon
        else
            None

    /// <summary>
    ///     Creates a new Worker agent instance with provided runtime configuration.
    /// </summary>
    /// <param name="runtimeManager">Runtime resource management object.</param>
    /// <param name="currentWorker">Worker ref for current instance.</param>
    /// <param name="jobEvaluator">Abstract job evaluator.</param>
    /// <param name="maxConcurrentJobs">Maximum number of jobs to be executed concurrently in this worker.</param>
    /// <param name="submitPerformanceMetrics">Enable or disable automatic performance metric submission to cluster worker manager.</param>
    static member Create(runtimeManager : IRuntimeManager, workerId : IWorkerId, jobEvaluator : ICloudJobEvaluator, maxConcurrentJobs : int, submitPerformanceMetrics:bool) = async {
        if maxConcurrentJobs < 1 then invalidArg "maxConcurrentJobs" "must be positive."
        let workerInfo =
            {
                Hostname = System.Net.Dns.GetHostName()
                ProcessorCount = Environment.ProcessorCount
                ProcessId = System.Diagnostics.Process.GetCurrentProcess().Id
                MaxJobCount = maxConcurrentJobs
            }

        let! unsubscriber = runtimeManager.WorkerManager.SubscribeWorker(workerId, workerInfo)
        return new WorkerAgent(runtimeManager, workerId, jobEvaluator, maxConcurrentJobs, unsubscriber, submitPerformanceMetrics)
    }

    /// Worker ref representing the current worker instance.
    member w.CurrentWorker = workerId

    /// Starts agent with supplied configuration
    member w.Start() = async {
        return! agent.PostAndAsyncReply Start
    }
        
    /// Removes current configuration from agent.
    member w.Stop(?timeout:int) = async {
        let timeout = defaultArg timeout Timeout.Infinite
        return! agent.PostAndAsyncReply (fun ch -> Stop(timeout, ch))
    }

    /// Gets the current number of jobs run by the worker
    member __.CurrentJobCount = currentJobCount
    /// Gets the maximum number of jobs allowed by the worker
    member __.MaxJobCount = maxConcurrentJobs

    /// Gets Current worker state configuration
    member w.CurrentStatus = agent.PostAndReply GetStatus

    /// Gets whether worker agent is currently running
    member w.IsRunning = 
        match w.CurrentStatus with WorkerJobExecutionStatus.Running | QueueFault _ -> true | _ -> false

    interface IDisposable with
        member w.Dispose () =
            if w.IsRunning then w.Stop(timeout = 5000) |> Async.RunSync
            unsubscriber.Dispose()
            cts.Cancel()
            perfmon |> Option.iter (fun pm -> (pm :> IDisposable).Dispose())