module MBrace.Thespian.StandaloneClient

open System
open MBrace.Core
open MBrace.Thespian

[<EntryPoint>]
let main argv =
    
    let config =
    #if DEBUG
        "Debug"
    #else
        "Release"
    #endif

    ThespianWorker.LocalExecutable <- (__SOURCE_DIRECTORY__ + "/../../src/MBrace.Thespian.Worker/bin/" + config + "/netcoreapp3.1/mbrace.thespian.worker.exe")

    let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 2, logger = new ConsoleLogger())

    let result = cloud { return "Hello world!" } |> cluster.Run

    printfn "Got result %A" result

    Console.ReadLine() |> ignore
    0 // return an integer exit code
