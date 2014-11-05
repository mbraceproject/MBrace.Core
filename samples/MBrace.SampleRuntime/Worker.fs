module internal Nessos.MBrace.SampleRuntime.Worker

open System.Diagnostics
open System.Threading

open Nessos.MBrace.SampleRuntime.Actors
open Nessos.MBrace.SampleRuntime.Tasks
open Nessos.MBrace.SampleRuntime.RuntimeProvider

/// Thread-safe printfn
let printfn fmt = Printf.ksprintf System.Console.WriteLine fmt

/// <summary>
///     Initializes a worker loop. Worker polls task queue of supplied
///     runtime for available tasks and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentTasks">Maximum tasks to be executed concurrently by worker.</param>
let initWorker (runtime : RuntimeState) (maxConcurrentTasks : int) = async {

    let localEndPoint = Nessos.MBrace.SampleRuntime.Config.getLocalEndpoint()
    printfn "MBrace worker initialized on %O." localEndPoint
    printfn "Listening to task queue at %O." runtime.IPEndPoint

    let currentTaskCount = ref 0
    let runTask deps t =
        let provider = RuntimeProvider.FromTask runtime deps t
        Task.RunAsync provider deps t

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 500
            return! loop ()
        else
            try
                let! task = runtime.TryDequeue()
                match task with
                | None -> do! Async.Sleep 500
                | Some (task, dependencies, leaseMonitor) ->
                    let _ = Interlocked.Increment currentTaskCount
                    let runTask () = async {
                        printfn "Starting task %s of type '%O'." task.Id task.Type

                        use hb = leaseMonitor.InitHeartBeat()

                        let sw = new Stopwatch()
                        sw.Start()
                        let! result = runTask dependencies task |> Async.Catch
                        sw.Stop()

                        match result with
                        | Choice1Of2 () -> 
                            leaseMonitor.Release()
                            printfn "Task %s completed after %O." task.Id sw.Elapsed
                                
                        | Choice2Of2 e -> 
                            leaseMonitor.DeclareFault()
                            printfn "Task %s faulted with:\n %O." task.Id e

                        let _ = Interlocked.Decrement currentTaskCount
                        return ()
                    }
        
                    let! handle = Async.StartChild(runTask())
                    return ()

            with e -> 
                printfn "WORKER FAULT: %O" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}
        