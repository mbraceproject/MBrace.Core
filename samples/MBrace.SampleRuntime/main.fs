module internal MBrace.SampleRuntime.Main

    open Nessos.Thespian
    open Nessos.Thespian.Remote.Protocols
    open MBrace.Continuation
    open MBrace.Runtime
    open MBrace.SampleRuntime.Actors

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            MBrace.SampleRuntime.Config.initRuntimeState()
            let address = MBrace.SampleRuntime.Config.getAddress()
            printfn "MBrace worker initialized on %O." address
            if args.Length > 0 then
                let runtime = Argument.toRuntime args
                Async.RunSync (Worker.initWorker runtime maxConcurrentTasks)
                0
            else
                Actor.Stateful (new System.Threading.CancellationTokenSource()) Worker.workerManager
                |> Actor.rename "workerManager"
                |> Actor.publish [ Protocols.utcp() ]
                |> Actor.start
                |> ignore
                Async.RunSynchronously <| async { while true do do! Async.Sleep 10000 }
                0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1
