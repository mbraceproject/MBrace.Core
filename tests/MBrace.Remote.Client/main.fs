module internal Nessos.MBrace.SampleRuntime.Main

    open System.Diagnostics

    open Nessos.MBrace.SampleRuntime.Scheduler

    [<EntryPoint>]
    let main (args : string []) =
        
        do Actors.Actor.InitClient()

        let parseResults = Argument.Parser.ParseCommandLine(args)
        let runtime = parseResults.PostProcessResult(<@ Pickled_Runtime @>, fun p -> Vagrant.vagrant.Pickler.UnPickle<RuntimeState> p)

        printfn "MBrace worker has been initialized, listening on task queue."

        let rec loop () : Async<int> = async {
            match runtime.TryDequeue () with
            | None ->
                do! Async.Sleep 20
                return! loop ()

            | Some task ->
                printfn "Executing task id %s of type '%O'" task.TaskId task.Type
                let sw = new Stopwatch()
                sw.Start()
                let result = try Task.Run runtime task ; None with e -> Some e
                sw.Stop()
                match result with
                | None -> printfn "Completed in %O." sw.Elapsed
                | Some e -> printfn "Fault %O." e
                return! loop ()
        }

        Async.RunSynchronously (loop ()) 