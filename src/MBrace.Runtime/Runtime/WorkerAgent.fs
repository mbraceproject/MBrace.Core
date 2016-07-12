namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils

[<NoEquality; NoComparison>]
type private WorkerAgentMessage = 
    | GetStatus of ReplyChannel<WorkerExecutionStatus>
    | Stop of waitTimeout:int * ReplyChannel<unit>
    | Start of ReplyChannel<unit>

/// Worker agent with updatable configuration
type WorkerAgent private (runtime : IRuntimeManager, workerId : IWorkerId, workItemEvaluator : ICloudWorkItemEvaluator, maxConcurrentWorkItems : int, unsubscriber : IDisposable, performanceMetricsInterval : int option) =
    let cts = new CancellationTokenSource()
    // TODO : keep a detailed record of work items, possibly add provision for forcible termination.
    let mutable currentWorkItemCount = 0

    let logger = runtime.SystemLogger
    let waitInterval = 100
    let errorInterval = 1000

    let rec workerLoop (status : WorkerExecutionStatus) (inbox : MailboxProcessor<WorkerAgentMessage>) = async {
        let! controlMessage = async {
            if inbox.CurrentQueueLength > 0 then
                let! m = inbox.Receive()
                return Some m
            else
                return None
        }

        let declareStatus status = async {
            try do! runtime.WorkerManager.DeclareWorkerStatus(workerId, status)
            with e -> logger.Logf LogLevel.Warning "Error updating worker status:\n%O" e
        }

        match controlMessage with
        | None ->
            match status with
            | Stopped ->
                do! Async.Sleep waitInterval
                return! workerLoop status inbox

            | _ when currentWorkItemCount >= maxConcurrentWorkItems ->
                do! Async.Sleep waitInterval
                return! workerLoop status inbox

            | _ ->
                let! workItem = Async.Catch <| runtime.WorkItemQueue.TryDequeue workerId
                match workItem with
                | Choice2Of2 e ->
                    let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                    let status = QueueFault edi
                    do! declareStatus status
                    logger.Logf LogLevel.Warning "Worker Work item Queue fault:\n%O" e
                    do! Async.Sleep errorInterval
                    return! workerLoop status inbox

                | Choice1Of2 result ->
                    let! status = async {
                        match status with
                        | QueueFault _ ->
                            let status = WorkerExecutionStatus.Running
                            logger.Log LogLevel.Info "Worker Work item Queue restored."
                            do! declareStatus status
                            return status

                        | _ -> return status
                    }

                    match result with
                    | None ->
                        do! Async.Sleep waitInterval
                        return! workerLoop status inbox

                    | Some workItemToken ->
                        // Successfully dequeued work item, run it.
                        if workItemToken.WorkItemType = CloudWorkItemType.ProcessRoot then
                            try do! workItemToken.Process.DeclareStatus CloudProcessStatus.WaitingToRun
                            with e -> logger.Logf LogLevel.Info "Error updating process status:\n%O" e

                        let jc = Interlocked.Increment &currentWorkItemCount
                        try do! runtime.WorkerManager.IncrementWorkItemCount workerId
                        with e -> logger.Logf LogLevel.Info "Error incrementing worker count:\n%O" e

                        logger.Logf LogLevel.Info "Dequeued work item '%O'." workItemToken.Id
                        logger.Logf LogLevel.Info "Concurrent work item count increased to %d/%d." jc maxConcurrentWorkItems

                        let! _ = Async.StartChild <| async { 
                            try
                                try
                                    let! assemblies = runtime.AssemblyManager.DownloadAssemblies workItemToken.Process.Info.Dependencies
                                    do! workItemEvaluator.Evaluate (assemblies, workItemToken)
                                with e ->
                                    do! workItemToken.Process.IncrementFaultedWorkItemCount()
                                    let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                                    do! workItemToken.DeclareFaulted edi
                                    logger.Logf LogLevel.Error "Work item '%O' faulted at initialization:\n%A" workItemToken.Id e
                                    return ()
                            finally
                                let jc = Interlocked.Decrement &currentWorkItemCount
                                runtime.WorkerManager.DecrementWorkItemCount workerId |> Async.Catch |> Async.Ignore |> Async.Start
                                logger.Logf LogLevel.Info "Concurrent work item count decreased to %d/%d." jc maxConcurrentWorkItems

                        }

                        do! Async.Sleep waitInterval
                        return! workerLoop status inbox

        | Some(Stop (waitTimeout, rc)) ->
            match status with
            | WorkerExecutionStatus.Running | QueueFault _ ->
                logger.Log LogLevel.Info "Stop requested. Waiting for pending work items."
                let rec wait () = async {
                    let jc = currentWorkItemCount
                    if jc > 0 then
                        logger.Logf LogLevel.Info "Waiting for (%d) active work items to complete." jc
                        do! Async.Sleep 1000
                        return! wait ()
                }

                let! result = Async.WithTimeout(wait (), waitTimeout) |> Async.Catch
                match result with
                | Choice1Of2 () -> logger.Log LogLevel.Info "No active work items."
                | Choice2Of2 _ -> logger.Logf LogLevel.Error "Timeout while waiting for active work items to complete."

                let status = Stopped
                do! declareStatus status
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
                let status = WorkerExecutionStatus.Running
                do! declareStatus status
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
        match performanceMetricsInterval with
        | None -> None
        | Some pi ->
            let perfmon = PerformanceMonitor.Start(cancellationToken = cts.Token)

            let rec perfLoop () = async {
                let perf = perfmon.GetCounters()
                try 
                    do! runtime.WorkerManager.SubmitPerformanceMetrics(workerId, perf)
                    do! Async.Sleep pi
                with e -> 
                    runtime.SystemLogger.Logf LogLevel.Error "Error submitting performance metrics:\n%O" e
                    do! Async.Sleep (3 * pi)

                return! perfLoop ()
            }

            Async.Start(perfLoop(), cts.Token)
            Some perfmon

    /// <summary>
    ///     Creates a new Worker agent instance with provided runtime configuration.
    /// </summary>
    /// <param name="runtimeManager">Runtime resource management object.</param>
    /// <param name="currentWorker">Worker ref for current instance.</param>
    /// <param name="workItemEvaluator">Abstract work item evaluator.</param>
    /// <param name="maxConcurrentWorkItems">Maximum number of work items to be executed concurrently in this worker.</param>
    /// <param name="submitPerformanceMetrics">Enable or disable automatic performance metric submission to cluster worker manager.</param>
    /// <param name="heartbeatInterval">Designated heartbeat interval sent by worker to the cluster.</param>
    /// <param name="heartbeatThreshold">Designated maximum heartbeat interval before declaring the cluster declares the worker dead.</param>
    static member Create(runtimeManager : IRuntimeManager, workerId : IWorkerId, workItemEvaluator : ICloudWorkItemEvaluator, 
                            maxConcurrentWorkItems : int, submitPerformanceMetrics:bool, heartbeatInterval : TimeSpan, heartbeatThreshold : TimeSpan) =                     
        async {
            if maxConcurrentWorkItems < 1 then invalidArg "maxConcurrentWorkItems" "must be positive."
            let performanceMetricsInterval = 
                if submitPerformanceMetrics then Some (int heartbeatInterval.TotalMilliseconds) 
                else None

            let workerInfo =
                {
                    Hostname = WorkerRef.CurrentHostname
                    ProcessorCount = Environment.ProcessorCount
                    ProcessId = System.Diagnostics.Process.GetCurrentProcess().Id
                    MaxWorkItemCount = maxConcurrentWorkItems
                    HeartbeatInterval = heartbeatInterval
                    HeartbeatThreshold = heartbeatThreshold
                }

            let! unsubscriber = runtimeManager.WorkerManager.SubscribeWorker(workerId, workerInfo)
            return new WorkerAgent(runtimeManager, workerId, workItemEvaluator, maxConcurrentWorkItems, unsubscriber, performanceMetricsInterval)
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

    /// Gets the current number of work items run by the worker
    member __.CurrentWorkItemCount = currentWorkItemCount
    /// Gets the maximum number of work items allowed by the worker
    member __.MaxWorkItemCount = maxConcurrentWorkItems

    /// Gets Current worker state configuration
    member w.CurrentStatus = agent.PostAndReply GetStatus

    /// Gets whether worker agent is currently running
    member w.IsRunning = 
        match w.CurrentStatus with WorkerExecutionStatus.Running | QueueFault _ -> true | _ -> false

    interface IDisposable with
        member w.Dispose () =
            if w.IsRunning then w.Stop(timeout = 5000) |> Async.RunSync
            unsubscriber.Dispose()
            cts.Cancel()
            perfmon |> Option.iter (fun pm -> Disposable.dispose pm)