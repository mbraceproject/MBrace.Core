namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils

[<NoEquality; NoComparison>]
type WorkerStatus =
    | Running
    | Stopped
    | QueueFault of ExceptionDispatchInfo

[<NoEquality; NoComparison>]
type WorkerState =
    {
        Status : WorkerStatus
        CurrentJobCount : int
        MaxJobCount : int
    }

[<NoEquality; NoComparison>]
type private WorkerAgentMessage = 
    | Stop of waitTimeout:int * ReplyChannel<unit>
    | Start of ReplyChannel<unit>

/// Worker agent with updatable configuration
type WorkerAgent private (resourceManager : IRuntimeResourceManager, jobEvaluator : ICloudJobEvaluator, maxConcurrentJobs : int) =
    let cts = new CancellationTokenSource()
    let event = new Event<WorkerState>()
    let mutable currentJobCount = 0
    [<VolatileField>]
    let mutable status = Stopped

    let getState () = { Status = status ; CurrentJobCount = currentJobCount ; MaxJobCount = maxConcurrentJobs }
    let triggerStateUpdate () = 
        let state = getState ()
        event.TriggerAsTask state |> ignore

    let logger = resourceManager.SystemLogger
    let waitInterval = 100
    let errorInterval = 1000

    let rec workerLoop (inbox : MailboxProcessor<WorkerAgentMessage>) = async {
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
                return! workerLoop inbox

            | _ when currentJobCount >= maxConcurrentJobs ->
                do! Async.Sleep waitInterval
                return! workerLoop inbox

            | _ ->
                let! job = Async.Catch <| resourceManager.JobQueue.TryDequeue resourceManager.CurrentWorker
                match job with
                | Choice2Of2 e ->
                    status <- QueueFault (ExceptionDispatchInfo.Capture e)
                    triggerStateUpdate ()

                    logger.Logf LogLevel.Info "Worker Job Queue fault:\n%O" e
                    do! Async.Sleep errorInterval
                    return! workerLoop inbox

                | Choice1Of2 result ->
                    match status with
                    | QueueFault _ ->
                        status <- Running
                        triggerStateUpdate ()
                        logger.Log LogLevel.Info "Worker Job Queue restored."

                    | _ -> ()

                    match result with
                    | None ->
                        do! Async.Sleep waitInterval
                        return! workerLoop inbox

                    | Some jobToken ->
                        // Successfully dequeued job, run it.
                        let jc = Interlocked.Increment &currentJobCount
                        triggerStateUpdate()

                        logger.Logf LogLevel.Info "Dequeued cloud job '%s'." jobToken.JobId
                        logger.Logf LogLevel.Info "Concurrent job count increased to %d/%d." jc maxConcurrentJobs

                        let! _ = Async.StartChild <| async { 
                            try
                                try
                                    let! assemblies = resourceManager.AssemblyManager.DownloadAssemblies jobToken.TaskInfo.Dependencies
                                    do! jobEvaluator.Evaluate (assemblies, jobToken)
                                with e ->
                                    logger.Logf LogLevel.Error "Job '%s' faulted at initialization:\n%A" jobToken.JobId e
                                    return ()
                            finally
                                let jc = Interlocked.Decrement &currentJobCount
                                triggerStateUpdate()
                                logger.Logf LogLevel.Info "Concurrent job count decreased to %d/%d." jc maxConcurrentJobs

                        }

                        do! Async.Sleep waitInterval
                        return! workerLoop inbox

        | Some(Stop (waitTimeout, rc)) ->
            match status with
            | Running | QueueFault _ ->
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

                status <- Stopped
                triggerStateUpdate()
                do rc.Reply (())

                logger.Log LogLevel.Info "Worker stopped."
                return! workerLoop inbox

            | _ ->
                rc.ReplyWithError (new InvalidOperationException("Worker is not running."))
                return! workerLoop inbox

        | Some(Start rc) ->
            match status with
            | Stopped ->
                logger.Log LogLevel.Info "Starting Worker."
                status <- Running
                triggerStateUpdate()
                do rc.Reply (())
                return! workerLoop inbox
            | _ ->
                rc.ReplyWithError (new InvalidOperationException "Worker is already running.")
                return! workerLoop inbox
    }

    let agent = MailboxProcessor.Start(workerLoop, cts.Token)

    /// <summary>
    ///     Creates a new Worker agent instance with provided runtime configuration.
    /// </summary>
    /// <param name="resourceManager">Runtime resource management object.</param>
    /// <param name="jobEvaluator">Abstract job evaluator.</param>
    /// <param name="maxConcurrentJobs">Maximum number of jobs to be executed concurrently in this worker.</param>
    static member Create(resourceManager : IRuntimeResourceManager, jobEvaluator : ICloudJobEvaluator, maxConcurrentJobs : int) =
        if maxConcurrentJobs < 1 then invalidArg "maxConcurrentJobs" "must be positive."
        new WorkerAgent(resourceManager, jobEvaluator, maxConcurrentJobs)

    /// Worker ref representing the current worker instance.
    member w.CurrentWorker = resourceManager.CurrentWorker

    /// Starts agent with supplied configuration
    member w.Start() = async {
        return! agent.PostAndAsyncReply Start
    }
        
    /// Removes current configuration from agent.
    member w.Stop(?timeout:int) = async {
        let timeout = defaultArg timeout Timeout.Infinite
        return! agent.PostAndAsyncReply (fun ch -> Stop(timeout, ch))
    }

    /// Gets Current worker state configuration
    member w.CurrentState = getState()

    /// Worker state change observable
    member w.OnStateChange = event.Publish

    /// Gets whether worker agent is currently running
    member w.IsRunning = 
        match status with Running | QueueFault _ -> true | _ -> false

    interface IDisposable with
        member w.Dispose () =
            if w.IsRunning then w.Stop(timeout = 5000) |> Async.RunSync
            cts.Cancel()