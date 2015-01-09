module internal MBrace.SampleRuntime.Worker

open System.Diagnostics
open System.Threading

open MBrace.Runtime
open MBrace.Runtime.Vagrant
open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Tasks
open MBrace.SampleRuntime.RuntimeProvider

/// Thread-safe printfn
let printfn fmt = Printf.ksprintf System.Console.WriteLine fmt

/// <summary>
///     Initializes a worker loop. Worker polls task queue of supplied
///     runtime for available tasks and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentTasks">Maximum tasks to be executed concurrently by worker.</param>
let initWorker (runtime : RuntimeState) (maxConcurrentTasks : int) = async {

    let localEndPoint = MBrace.SampleRuntime.Config.getLocalEndpoint()
    //printfn "MBrace worker initialized on %O." localEndPoint
    printfn "Listening to task queue at %O." runtime.IPEndPoint

    let currentTaskCount = ref 0
    let runTask procId deps faultCount t =
        let runtimeProvider = RuntimeProvider.FromTask runtime procId deps t
        let channelProvider = new ActorChannelProvider(runtime)
        Task.RunAsync runtimeProvider channelProvider deps faultCount t

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 500
            return! loop ()
        else
            try
                let! task = runtime.TryDequeue()
                match task with
                | None -> do! Async.Sleep 500
                | Some (task, dependencies, faultCount, leaseMonitor) ->
                    let _ = Interlocked.Increment currentTaskCount
                    let runTask () = async {
                        printfn "Starting task %s of type '%O'." task.TaskId task.Type

                        use! hb = leaseMonitor.InitHeartBeat()

                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = runTask task.ProcessInfo dependencies faultCount task |> Async.Catch
                        sw.Stop()

                        match result with
                        | Choice1Of2 () -> 
                            leaseMonitor.Release()
                            printfn "Task %s completed after %O." task.TaskId sw.Elapsed
                                
                        | Choice2Of2 e -> 
                            leaseMonitor.DeclareFault()
                            printfn "Task %s faulted with:\n %O." task.TaskId e

                        let _ = Interlocked.Decrement currentTaskCount
                        return ()
                    }
        
                    let! handle = Async.StartChild(runTask())
                    do! Async.Sleep 200

            with e -> 
                printfn "WORKER FAULT: %O" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}

open Nessos.Thespian

let workerManager (cts: CancellationTokenSource) (msg: WorkerManager) =
    async {
        match msg with
        | SubscribeToRuntime(rc, runtimeStateStr, maxConcurrentTasks) ->
            let runtimeState =
                let bytes = System.Convert.FromBase64String(runtimeStateStr)
                VagrantRegistry.Pickler.UnPickle<RuntimeState> bytes

            Async.Start(initWorker runtimeState maxConcurrentTasks, cts.Token)
            try do! rc.Reply() with e -> printfn "Failed to confirm worker subscription to client: %O" e
            return cts
        | Unsubscribe rc ->
            cts.Cancel()
            try do! rc.Reply() with e -> printfn "Failed to confirm worker unsubscription to client: %O" e
            return new CancellationTokenSource()
    }
