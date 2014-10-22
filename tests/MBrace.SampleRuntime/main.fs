module internal Nessos.MBrace.SampleRuntime.Main

    open System.Diagnostics

    open Nessos.MBrace.SampleRuntime.PortablePickle
    open Nessos.MBrace.SampleRuntime.Scheduler

    [<EntryPoint>]
    let main (args : string []) =
        
        do Actors.Actor.InitClient()

        let parseResults = Argument.Parser.ParseCommandLine(args)
        let runtime = parseResults.GetAllResults() |> List.head |> Argument.ToRuntime

        printfn "MBrace worker has been initialized, listening on task queue."

        let rec loop () = async {
            match runtime.TryDequeue () with
            | None ->
                do! Async.Sleep 100
                return! loop ()

            | Some task ->
                printfn "Executing task id %s of type '%O'" task.TaskId task.Type

                let sw = new Stopwatch()
                sw.Start()
                let! result = Async.Catch <| Task.RunAsync runtime task
                sw.Stop()

                match result with
                | Choice1Of2 () -> printfn "Completed in %O." sw.Elapsed
                | Choice2Of2 e -> printfn "Fault %O." e

                return! loop ()
        }

        Async.RunSynchronously (loop ()) 