module internal Nessos.MBrace.Runtime.Azure.Main

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            Nessos.MBrace.Runtime.Azure.Config.initRuntimeState()
            let runtime = Argument.toRuntime args
            Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1