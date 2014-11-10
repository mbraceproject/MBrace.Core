module internal Nessos.MBrace.SampleRuntime.Main

    open Nessos.MBrace.Runtime

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            Nessos.MBrace.SampleRuntime.Config.initRuntimeState()
            let runtime = Argument.toRuntime args
            Async.RunSync (Worker.initWorker runtime maxConcurrentTasks)
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1