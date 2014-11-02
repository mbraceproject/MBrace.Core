module internal Nessos.MBrace.SampleRuntime.Main

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            Nessos.MBrace.SampleRuntime.Config.initRuntimeState()
            let runtime = Argument.toRuntime args
            Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %A" e
            let _ = System.Console.ReadKey()
            1