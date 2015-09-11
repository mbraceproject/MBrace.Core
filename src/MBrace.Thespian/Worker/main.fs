module MBrace.Thespian.Runtime.Worker

open System
open System.IO

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols
open MBrace.Core.Internals
open MBrace.Runtime

let maxConcurrentWorkItems = 20
let useAppDomainIsolation = true

let main (args : string []) =
    try
        let hostname = System.Net.Dns.GetHostName()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id

        let config = WorkerConfiguration.Parse(args)
        let logger = AttacheableLogger.Create(logLevel = defaultArg config.LogLevel LogLevel.Info, makeAsynchronous = (config.LogFiles.Length > 0))
        Actor.Logger <- logger

        try
            do Config.Initialize(populateDirs = true, isClient = false, ?workingDirectory = config.WorkingDirectory, ?hostname = config.Hostname, ?port = config.Port)
            Console.Title <- sprintf "MBrace.Thespian Worker [pid:%d, port:%d]" pid Config.LocalTcpPort

            let _ = logger.AttachLogger (new ConsoleLogger(useColors = true))
            for file in config.LogFiles do
                let path = Path.Combine(Config.WorkingDirectory, file)
                let fl = FileSystemLogger.Create(path, showDate = true, append = true)
                let _ = logger.AttachLogger fl
                ()

            logger.Logf LogLevel.Info "Initializing MBrace.Thespian Worker node."
            logger.Logf LogLevel.Info "Hostname: %s" hostname
            logger.Logf LogLevel.Info "Process Id: %d" pid
            logger.Logf LogLevel.Info "Thespian initialized with listener %s" Config.LocalAddress

            let controller = 
                WorkerController.initController 
                    (defaultArg config.UseAppDomainIsolation useAppDomainIsolation) 
                    (defaultArg config.MaxConcurrentWorkItems maxConcurrentWorkItems)
                    logger

            replyToParent logger config (Success controller)

        with e ->
            replyToParent logger config (ProcessError e)
            reraise()

        while true do System.Threading.Thread.Sleep 10000
        0

    with e ->
        printfn "Unhandled exception : %O" e
        printfn "Press any key to exit..."
        let _ = System.Console.ReadKey()
        1