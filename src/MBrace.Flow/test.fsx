#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Thespian.dll"
#r "MBrace.Flow.dll"
#r "Streams.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow
open MBrace.Flow.Fluent
open MBrace.ThreadPool




ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.thespian.worker.exe"
let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 4, logLevel = LogLevel.Debug, logger = new ConsoleLogger())

let source = [| 1; 3; 1; 4; 2; 5; 2; 42; 42; 7; 8; 10; 8;|]

CloudFlow.OfArray source
|> CloudFlow.distinctBy id
|> CloudFlow.toArray
|> cluster.Run


for i in 1..100 do
    let source = [| 1..10 |]
    let q = source |> CloudFlow.OfArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray
    printfn "%A" <| cluster.Run q

let persisted = 
    CloudFlow.OfArray([|1 .. 1000|])
             .collect(fun i -> [|1..10000|] |> Seq.map (fun j -> string i, j))
             .persist StorageLevel.Disk
    |> cluster.Run

persisted |> CloudFlow.length |> cluster.Run