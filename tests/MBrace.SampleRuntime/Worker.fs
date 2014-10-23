module internal Nessos.MBrace.SampleRuntime.Worker

    open System
    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace.SampleRuntime.Scheduler

    let printfn fmt = Printf.ksprintf System.Console.WriteLine fmt

    let initWorker (runtime : RuntimeState) (maxConcurrentTasks : int) = async {
        let currentTaskCount = ref 0

        printfn "Listening to task queue at %O." runtime.IPEndPoint

        let rec loop () = async {
            if !currentTaskCount >= maxConcurrentTasks then
                do! Async.Sleep 20
                return! loop ()
            else
                try
                    match runtime.TryDequeue() with
                    | None -> do! Async.Sleep 50
                    | Some task ->
                        let _ = Interlocked.Increment currentTaskCount
                        let runTask () = async {
                            printfn "Starting task %s of type '%O'." task.Id task.Type

                            let sw = new Stopwatch()
                            sw.Start()
                            let! result = Async.Catch <| Task.RunAsync runtime task
                            sw.Stop()

                            match result with
                            | Choice1Of2 () -> printfn "Task %s completed after %O." task.Id sw.Elapsed
                            | Choice2Of2 e -> printfn "Task %s faulted with:\n %O." task.Id e

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
        