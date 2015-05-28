namespace MBrace.Runtime

open System
open System.Threading

open Microsoft.FSharp.Control

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond

type IWorkerManager =
    abstract CurrentWorker : IWorkerRef
    abstract SetCurrentAsRunning : unit -> Async<unit>
    abstract SetCurrentAsStopped : unit -> Async<unit>
    abstract SetCurrentAsFaulted : exn:exn -> Async<unit>
    abstract SetJobCount : jobCount:int -> unit

[<NoEquality; NoComparison>]
type WorkerConfig = 
    { 
        MaxConcurrentJobs   : int
        OnErrorWaitTime     : int

        JobQueue            : IJobQueue
        WorkerManager       : IWorkerManager
        AssemblyManager     : StoreAssemblyManager
        SystemLogger        : ICloudLogger
        JobEvaluator        : ICloudJobEvaluator
    }

[<NoEquality; NoComparison>]
type private WorkerMessage =
    | Start of WorkerConfig  * ReplyChannel<unit>
    | Stop of ReplyChannel<unit>
    | IsActive of ReplyChannel<bool>

[<NoEquality; NoComparison>]
type private WorkerState =
    | Idle
    | Running of WorkerConfig * ReplyChannel<unit>

type WorkerAgent () =

    let receiveTimeout = 100
    let mutable currentJobCount = 0

    let waitForPendingJobs (config : WorkerConfig) = async {
        config.SystemLogger.Log "Stop requested. Waiting for pending jobs."
        let rec wait () = async {
            if currentJobCount > 0 then
                do! Async.Sleep receiveTimeout
                return! wait ()
        }
        do! wait ()
        config.SystemLogger.Log "No active jobs."
        config.SystemLogger.Log "Unregister current worker."
        do! config.WorkerManager.SetCurrentAsStopped()
        config.SystemLogger.Log "Worker stopped."
    }

    let rec workerLoop (queueFault : bool) (state : WorkerState) 
                        (inbox : MailboxProcessor<WorkerMessage>) = async {

        let! message = inbox.TryReceive(timeout = receiveTimeout)
        match message, state with
        | None, Running(config, _) ->
            if currentJobCount >= config.MaxConcurrentJobs then
                do! Async.Sleep receiveTimeout
                return! workerLoop false state inbox
            else
                let! job = Async.Catch <| config.JobQueue.TryDequeue config.WorkerManager.CurrentWorker
                match job with
                | Choice1Of2 None -> 
                    if queueFault then 
                        config.SystemLogger.Log "Reverting state to Running"
                        do! config.WorkerManager.SetCurrentAsRunning()
                        config.SystemLogger.Log "Done"

                    return! workerLoop false state inbox

                | Choice1Of2(Some jobToken) ->
                    // run job
                    let jc = Interlocked.Increment &currentJobCount
                    if queueFault then 
                        config.SystemLogger.Log "Reverting state to Running"
                        do! config.WorkerManager.SetCurrentAsRunning()
                        config.SystemLogger.Logf "Done"
                    config.WorkerManager.SetJobCount(jc)
                    config.SystemLogger.Logf "Increase Dequeued Jobs to %d." jc
                    let! _ = Async.StartChild <| async { 
                        try
                            let! assemblies = config.AssemblyManager.DownloadDependencies jobToken.Dependencies
                            do! config.JobEvaluator.Evaluate (List.toArray assemblies, jobToken)
                        finally
                            let jc = Interlocked.Decrement &currentJobCount
                            config.WorkerManager.SetJobCount(jc)
                            config.SystemLogger.Logf "Decrease Dequeued Jobs %d" jc
                    }

                    return! workerLoop false state inbox

                | Choice2Of2 ex ->
                    config.SystemLogger.Logf "Worker JobQueue fault\n%A" ex
                    do! config.WorkerManager.SetCurrentAsFaulted(ex)
                    do! Async.Sleep config.OnErrorWaitTime
                    return! workerLoop true state inbox

        | None, Idle -> 
            do! Async.Sleep receiveTimeout
            return! workerLoop false state inbox

        | Some(Start(config, handle)), Idle ->
            return! workerLoop false (Running(config, handle)) inbox

        | Some(Start (_,ch)), Running _  ->
            let e = new InvalidOperationException("Called Start, but worker is not Idle.")
            ch.ReplyWithError e
            return! workerLoop queueFault state inbox

        | Some(Stop ch), Running(config, handle) ->
            do! waitForPendingJobs config
            ch.Reply (())
            handle.Reply(())
            return! workerLoop false Idle inbox

        | Some (Stop ch), Idle ->
            let e = new InvalidOperationException("Worker is Idle, cannot stop.")
            ch.ReplyWithError e
            return! workerLoop queueFault state inbox

        | Some(IsActive ch), Idle ->
            ch.Reply(false)
            return! workerLoop false state inbox

        | Some(IsActive ch), Running _ ->
            ch.Reply(true)
            return! workerLoop false state inbox
    }

    let agent = MailboxProcessor.Start(workerLoop false Idle)
    
    member __.IsActive = agent.PostAndReply(IsActive)

    member __.Start(configuration : WorkerConfig) =
        agent.PostAndReply(fun ch -> Start(configuration, ch))
        
    member __.Stop() = agent.PostAndReply(fun ch -> Stop(ch))

    member __.Restart(configuration) =
        __.Stop()
        __.Start(configuration)