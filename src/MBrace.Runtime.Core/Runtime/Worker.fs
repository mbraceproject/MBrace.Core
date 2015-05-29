namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond

/// Worker management object used for updating with execution metadata.
type IWorkerManager =
    /// Gets the current worker reference object.
    abstract CurrentWorker : IWorkerRef
    /// Declare current worker instance as running.
    abstract DeclareRunning : unit -> Async<unit>
    /// Declare current worker instance as stopped.
    abstract DeclareStopped : unit -> Async<unit>
    /// Declare current worker instance as faulted.
    abstract DeclareFaulted : exn -> Async<unit>
    /// Declare the current job count for worker instance.
    abstract DeclareJobCount : jobCount:int -> Async<unit>

/// Worker configuration record.
[<NoEquality; NoComparison>]
type WorkerConfig = 
    { 
        /// Maximum jobs allowed for concurrent execution
        MaxConcurrentJobs   : int
        /// Cloud Job queue implementation
        JobQueue            : IJobQueue
        /// Worker manager imeplementation
        WorkerManager       : IWorkerManager
        /// Vagabond store assembly manager implementation
        AssemblyManager     : StoreAssemblyManager
        /// System logger for worker instance
        SystemLogger        : ICloudLogger
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

/// Worker agent with updatable configuration
type WorkerAgent private (?receiveInterval : TimeSpan, ?errorInterval : TimeSpan) =

    let cts = new CancellationTokenSource()
    let receiveInterval = match receiveInterval with Some t -> int t.TotalMilliseconds | None -> 100
    let errorInterval = match errorInterval with Some t -> int t.TotalMilliseconds | None -> 1000
    let mutable currentJobCount = 0

    let waitForPendingJobs (config : WorkerConfig) = async {
        config.SystemLogger.Log "Stop requested. Waiting for pending jobs."
        let rec wait () = async {
            if currentJobCount > 0 then
                do! Async.Sleep receiveInterval
                return! wait ()
        }
        do! wait ()
        config.SystemLogger.Log "No active jobs."
        config.SystemLogger.Log "Unregister current worker."
        do! config.WorkerManager.DeclareStopped()
        config.SystemLogger.Log "Worker stopped."
    }

    let rec workerLoop (queueFault : bool) (state : WorkerState) 
                        (inbox : MailboxProcessor<WorkerMessage>) = async {

        let! message = inbox.TryReceive()
        match message, state with
        | None, Running config ->
            if currentJobCount >= config.MaxConcurrentJobs then
                do! Async.Sleep receiveInterval
                return! workerLoop false state inbox
            else
                let! job = Async.Catch <| config.JobQueue.TryDequeue config.WorkerManager.CurrentWorker
                match job with
                | Choice1Of2 None ->
                    if queueFault then 
                        config.SystemLogger.Log "Reverting state to Running"
                        do! config.WorkerManager.DeclareRunning()
                        config.SystemLogger.Log "Done"

                    do! Async.Sleep receiveInterval
                    return! workerLoop false state inbox

                | Choice1Of2(Some jobToken) ->
                    // run job
                    let jc = Interlocked.Increment &currentJobCount

                    if queueFault then 
                        config.SystemLogger.Log "Reverting state to Running"
                        do! config.WorkerManager.DeclareRunning()
                        config.SystemLogger.Logf "Done"

                    do! config.WorkerManager.DeclareJobCount jc
                    config.SystemLogger.Logf "Increase Dequeued Jobs to %d." jc
                    let! _ = Async.StartChild <| async { 
                        try
                            let! assemblies = config.AssemblyManager.DownloadDependencies jobToken.Dependencies
                            do! config.JobEvaluator.Evaluate (List.toArray assemblies, jobToken)
                        finally
                            let jc = Interlocked.Decrement &currentJobCount
                            config.WorkerManager.DeclareJobCount jc |> Async.RunSync
                            config.SystemLogger.Logf "Decrease Dequeued Jobs %d" jc
                    }

                    return! workerLoop false state inbox

                | Choice2Of2 ex ->
                    config.SystemLogger.Logf "Worker JobQueue fault\n%A" ex
                    do! config.WorkerManager.DeclareFaulted ex
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