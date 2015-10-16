module MBrace.Thespian.Runtime.Worker

open System
open System.IO
open System.Threading

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Thespian.Runtime

let maxConcurrentWorkItems = 20
let useAppDomainIsolation = true
let heartbeatInterval = TimeSpan.FromSeconds 1.
let heartbeatThreshold = TimeSpan.FromSeconds 30.

let main (args : string []) =
    try
        let hostname = System.Net.Dns.GetHostName()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id

        let config = WorkerConfiguration.Parse(args)
        let logger = AttacheableLogger.Create(logLevel = defaultArg config.LogLevel LogLevel.Info, makeAsynchronous = (config.LogFiles.Length > 0))
        Actor.Logger <- logger

        try
            do Config.Initialize(populateDirs = true, isClient = false, ?workingDirectory = config.WorkingDirectory, ?hostname = config.Hostname, ?port = config.Port)
            Config.SetConsoleTitle()

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
                    (defaultArg config.HeartbeatInterval heartbeatInterval)
                    (defaultArg config.HeartbeatThreshold heartbeatThreshold)
                    logger

            replyToParent logger config (Success controller)

        with e ->
            replyToParent logger config (ProcessError e)
            reraise()

        Thread.Diverge()

    with e ->
        printfn "Unhandled exception : %O" e
        printfn "Press any key to exit..."
        let _ = System.Console.ReadKey()
        1
