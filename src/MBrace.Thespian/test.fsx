﻿#I "../../tests/MBrace.Thespian.Tests/bin/Debug/netcoreapp3.1"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Thespian.dll"
#r "Streams.dll"
#r "MBrace.Flow.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow

ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../MBrace.Thespian.Worker/bin/Debug/netcoreapp3.1/mbrace.thespian.worker"

#time "on"

let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 4, logLevel = LogLevel.Debug)
cluster.AttachLogger(new ConsoleLogger())

let workers = cluster.Workers

cloud { return 42 } |> cluster.Run
cloud { return 42 } |> cluster.RunLocally

cluster.ShowProcesses()
cluster.ShowWorkers()
cluster.ClearSystemLogs()
cluster.ShowSystemLogs()

let cloudProcess = 
    CloudFlow.OfHttpFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> cluster.CreateProcess

cloudProcess.Result


let test = cloud {
    let cell = ref 0
    let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { incr cell } ]
    return !cell
}

cluster.RunLocally(test, memoryEmulation = MemoryEmulation.Shared)
cluster.RunLocally(test, memoryEmulation = MemoryEmulation.Copied)
cluster.Run test

let test' = cloud {
    return box(new System.IO.MemoryStream())
}

cluster.RunLocally(test', memoryEmulation = MemoryEmulation.Shared)
cluster.RunLocally(test', memoryEmulation = MemoryEmulation.EnsureSerializable)
cluster.Run test'

let pflow =
    CloudFlow.OfArray [|1 .. 100|]
    |> CloudFlow.collect (fun i -> seq { for j in 1L .. 10000L -> int64 i * j })
    |> CloudFlow.filter (fun i -> i % 3L <> 0L)
    |> CloudFlow.map (fun i -> sprintf "Lorem ipsum dolor sit amet #%d" i)
    |> CloudFlow.cache
    |> cluster.Run

pflow |> CloudFlow.length |> cluster.Run


cloud {
    let! p1 = Cloud.CreateProcess(cloud { let! _ = Cloud.Sleep 10000 in return 1 })
    let! p2 = Cloud.CreateProcess(cloud { let! _ = Cloud.Sleep 20000 in return 2 })
    return! Cloud.WhenAny(p1, p2)
} |> cluster.Run


let mutable x = 0
for i in 1 .. 10 do
    x <- x + 1
    let y = cluster.Run( cloud { return x })
    printfn "%d" y