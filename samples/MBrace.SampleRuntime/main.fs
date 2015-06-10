module internal MBrace.SampleRuntime.Main

open System

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols
open MBrace.Core.Internals
open MBrace.Runtime

let maxConcurrentJobs = 10
let useAppDomainIsolation = true

[<EntryPoint>]
let main (args : string []) =
    try
        let hostname = System.Net.Dns.GetHostName()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id
        Console.Title <- sprintf "MBrace SampleRuntime (%s, Pid:%d)" hostname pid 
        Console.WriteLine "##### MBrace SampleRuntime #####"


        let logger = new ConsoleSystemLogger()
        logger.Logf LogLevel.Info "Hostname: %s" hostname
        logger.Logf LogLevel.Info "Process Id: %d" pid

        do Config.Init(populateDirs = true)
        logger.Logf LogLevel.Info "Thespian initialized with address: %s" Config.LocalAddress

        let state = RuntimeState.FromBase64 args.[0]
        logger.Logf LogLevel.Info "Connected to MBrace cluster '%s' at %s. " state.Id state.Address

        let agent, subscription =
            Worker.initialize useAppDomainIsolation state logger maxConcurrentJobs 
            |> Async.RunSync
            
        while true do System.Threading.Thread.Sleep 10000
        0

    with e ->
        printfn "Unhandled exception : %O" e
        let _ = System.Console.ReadKey()
        1
