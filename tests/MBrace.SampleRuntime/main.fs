module internal Nessos.MBrace.SampleRuntime.Main

    let maxConcurrentTasks = 20

    [<EntryPoint>]
    let main (args : string []) =
        try
            do Actors.Actor.LocalEndPoint |> ignore // force actor state initialization
            let runtime = Argument.toRuntime args
            Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1