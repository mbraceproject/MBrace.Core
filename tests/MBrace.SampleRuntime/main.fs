module internal Nessos.MBrace.SampleRuntime.Main

    let maxConcurrentTasks = 20

    [<EntryPoint>]
    let main (args : string []) =
        do Actors.Actor.LocalEndPoint |> ignore // force actor initializations

        let parseResults = Argument.Parser.ParseCommandLine(args)
        let runtime = parseResults.GetAllResults() |> List.head |> Argument.ToRuntime

        printfn "MBrace worker initialized on %O." Actors.Actor.LocalEndPoint

        Async.RunSynchronously (Worker.initWorker runtime maxConcurrentTasks)