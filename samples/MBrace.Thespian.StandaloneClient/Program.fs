open System
open MBrace.Core
open MBrace.Thespian

[<EntryPoint>]
let main argv = 

    ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.thespian.worker.exe"

    let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 2, logger = new ConsoleLogger())

    let result = cloud { return "Hello world!" } |> cluster.Run

    printfn "Got result %A" result

    Console.ReadLine() |> ignore
    0 // return an integer exit code
