module internal Nessos.MBrace.SampleRuntime.Worker

    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace.SampleRuntime.Actors
    open Nessos.MBrace.SampleRuntime.RuntimeTypes
    open Nessos.MBrace.SampleRuntime.RuntimeProvider

    let printfn fmt = Printf.ksprintf System.Console.WriteLine fmt

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
                do! Async.Sleep 100
                return! loop ()
            else
                try
                    let! task = runtime.TryDequeue()
                    match task with
                    | None -> do! Async.Sleep 100
                    | Some (task, dependencies, leaseMonitor) ->
                        let _ = Interlocked.Increment currentTaskCount
                        let runTask () = async {
                            printfn "Starting task %s of type '%O'." task.Id task.Type

                            let hb = leaseMonitor.InitHeartBeat()

                            let sw = new Stopwatch()
                            sw.Start()
                            let! result = runTask dependencies task |> Async.Catch
                            sw.Stop()

                            match result with
                            | Choice1Of2 () -> 
                                leaseMonitor.SetLeaseState Released
                                printfn "Task %s completed after %O." task.Id sw.Elapsed
                                
                            | Choice2Of2 e -> 
                                leaseMonitor.SetLeaseState Faulted
                                printfn "Task %s faulted with:\n %O." task.Id e

                            hb.Dispose()
                            let _ = Interlocked.Decrement currentTaskCount
                            return ()
                        }
        
                        let! handle = Async.StartChild(runTask())
                        return ()

                with e -> 
                    printfn "RUNTIME FAULT: %O" e
                    do! Async.Sleep 1000

                return! loop ()
        }

        return! loop ()
    }
        