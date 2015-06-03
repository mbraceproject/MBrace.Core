module internal MBrace.SampleRuntime.Main

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols
open MBrace.Core.Internals
open MBrace.Runtime

let maxConcurrentJobs = 10

[<EntryPoint>]
let main (args : string []) =
    try
        do Config.Init()
        let logger = new ConsoleLogger()
        Config.Logger <- logger

        let worker =
#if APPDOMAIN_ISOLATION
            Worker.InitLocal(logger, maxConcurrentJobs, useAppDomainIsolation = true)
#else
            Worker.InitLocal(logger, maxConcurrentJobs, useAppDomainIsolation = false)
#endif
        if args.Length > 0 then
            let state = Argument.toRuntime args
            worker.Subscribe state
            
        while true do System.Threading.Thread.Sleep 10000
        0

    with e ->
        printfn "Unhandled exception : %O" e
        let _ = System.Console.ReadKey()
        1
