namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils

/// Worker management object used for updating with execution metadata.
type IWorkerManager =
    /// Gets the current worker reference object.
    abstract CurrentWorker : IWorkerRef
    /// Declare current worker instance as running.
    abstract DeclareRunning : unit -> Async<unit>
    /// Declare current worker instance as stopped.
    abstract DeclareStopped : unit -> Async<unit>
    /// Declare current worker instance as faulted.
    abstract DeclareFaulted : ExceptionDispatchInfo -> Async<unit>
    /// Declare the current job count for worker instance.
    abstract DeclareJobCount : jobCount:int -> Async<unit>
    

/// Worker configuration record.
[<NoEquality; NoComparison>]
type WorkerConfig = 
    { 
        /// Maximum jobs allowed for concurrent execution
        MaxConcurrentJobs   : int
        /// Runtime resource manager
        Resources           : IRuntimeResourceManager
        /// Worker manager implementation
        WorkerManager       : IWorkerManager
        /// Job evaluator implementation
        JobEvaluator        : ICloudJobEvaluator
    }

[<NoEquality; NoComparison>]
type private WorkerMessage =
    | Start of WorkerConfig  * ReplyChannel<unit>
    | Stop of ReplyChannel<unit>
    | Restart of WorkerConfig * ReplyChannel<unit>
    | IsActive of ReplyChannel<WorkerConfig option>

[<NoEquality; NoComparison>]
type private WorkerState =
    | Idle
    | Running of WorkerConfig

// TODO: remove agent logic and pass workerconfig as ctor argument with proper disposal

/// Worker agent with updatable configuration
type WorkerAgent private (?receiveInterval : TimeSpan, ?errorInterval : TimeSpan) =

    let cts = new CancellationTokenSource()
    let receiveInterval = match receiveInterval with Some t -> int t.TotalMilliseconds | None -> 100
    let errorInterval = match errorInterval with Some t -> int t.TotalMilliseconds | None -> 1000
    let mutable currentJobCount = 0

    let waitForPendingJobs (config : WorkerConfig) = async {
        let logger = config.Resources.SystemLogger
        logger.Log LogLevel.Info "Stop requested. Waiting for pending jobs."
        let rec wait () = async {
            if currentJobCount > 0 then
                do! Async.Sleep receiveInterval
                return! wait ()
        }
        do! wait ()
        logger.Log LogLevel.Info "No active jobs."
        logger.Log LogLevel.Info "Unregister current worker."
        do! config.WorkerManager.DeclareStopped()
        logger.Log LogLevel.Info "Worker stopped."
    }

    let rec workerLoop (queueFault : bool) (state : WorkerState) 
                        (inbox : MailboxProcessor<WorkerMessage>) = async {

        let! message = inbox.TryReceive(timeout = 10)
        match message, state with
        | None, Running config ->
            let logger = config.Resources.SystemLogger
            if currentJobCount >= config.MaxConcurrentJobs then
                do! Async.Sleep receiveInterval
                return! workerLoop false state inbox
            else
                let! job = Async.Catch <| config.Resources.JobQueue.TryDequeue config.WorkerManager.CurrentWorker
                match job with
                | Choice1Of2 None ->
                    if queueFault then 
                        logger.Log LogLevel.Info "Reverting state to Running"
                        do! config.WorkerManager.DeclareRunning()
                        logger.Log LogLevel.Info "Done"

                    do! Async.Sleep receiveInterval
                    return! workerLoop false state inbox

                | Choice1Of2(Some jobToken) ->
                    // run job
                    let jc = Interlocked.Increment &currentJobCount

                    if queueFault then 
                        logger.Log LogLevel.Info "Reverting state to Running"
                        do! config.WorkerManager.DeclareRunning()
                        logger.Log LogLevel.Info "Done"

                    do! config.WorkerManager.DeclareJobCount jc
                    logger.Logf LogLevel.Info "Increase Dequeued Jobs to %d." jc
                    let! _ = Async.StartChild <| async { 
                        try
                            logger.Log LogLevel.Info "Downloading dependencies."
                            let! assemblies = config.Resources.AssemblyManager.DownloadAssemblies jobToken.Dependencies
                            do! config.JobEvaluator.Evaluate (assemblies, jobToken)
                        finally
                            let jc = Interlocked.Decrement &currentJobCount
                            config.WorkerManager.DeclareJobCount jc |> Async.RunSync
                            logger.Logf LogLevel.Info "Decrease Dequeued Jobs %d" jc
                    }

                    return! workerLoop false state inbox

                | Choice2Of2 ex ->
                    logger.Logf LogLevel.Info "Worker JobQueue fault\n%A" ex
                    do! config.WorkerManager.DeclareFaulted (ExceptionDispatchInfo.Capture ex)
                    do! Async.Sleep errorInterval
                    return! workerLoop true state inbox

        | None, Idle -> 
            do! Async.Sleep receiveInterval
            return! workerLoop false state inbox

        | Some(Start(config, ch)), Idle ->
            do ch.Reply (())
            return! workerLoop false (Running config) inbox

        | Some(Start (_,ch)), Running _  ->
            let e = new InvalidOperationException("Called Start, but worker is not Idle.")
            ch.ReplyWithError e
            return! workerLoop queueFault state inbox

        | Some(Stop ch), Running config ->
            do! waitForPendingJobs config
            ch.Reply (())
            return! workerLoop false Idle inbox

        | Some(Stop ch), Idle ->
            let e = new InvalidOperationException("Worker is Idle, cannot stop.")
            ch.ReplyWithError e
            return! workerLoop queueFault state inbox

        | Some(Restart (config, ch)), Idle ->
            ch.Reply (())
            return! workerLoop queueFault (Running config) inbox

        | Some(Restart (config, ch)), Running _ ->
            do! waitForPendingJobs config
            ch.Reply (())
            return! workerLoop false (Running config) inbox

        | Some(IsActive ch), Idle ->
            ch.Reply None
            return! workerLoop false state inbox

        | Some(IsActive ch), Running config ->
            ch.Reply (Some config)
            return! workerLoop false state inbox
    }

    let agent = MailboxProcessor.Start(workerLoop false Idle, cts.Token)

    /// <summary>
    ///     Creates a blank Worker agent instance.
    /// </summary>
    /// <param name="receiveInterval">Queue message receive interval. Defaults to 100ms.</param>
    /// <param name="errorInterval">Queue error wait interval. Defaults to 1000ms.</param>
    static member Create(?receiveInterval : TimeSpan, ?errorInterval : TimeSpan) =
        new WorkerAgent(?receiveInterval = receiveInterval, ?errorInterval = errorInterval)
    
    /// Checks whether worker agent is active.
    member w.IsActive = agent.PostAndReply IsActive |> Option.isSome

    /// Gets the current configuration for the agent.
    member w.Configuration = agent.PostAndReply IsActive

    /// Starts agent with supplied configuration
    member w.Start(configuration : WorkerConfig) =
        agent.PostAndReply(fun ch -> Start(configuration, ch))
        
    /// Removes current configuration from agent.
    member w.Stop() = agent.PostAndReply(fun ch -> Stop(ch))

    /// Restarts agent with new configuration
    member w.Restart(configuration : WorkerConfig) = agent.PostAndReply(fun ch -> Restart(configuration,ch))

    interface IDisposable with
        member w.Dispose () = w.Stop() ; cts.Cancel()