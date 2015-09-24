#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Thespian.dll"
#r "MBrace.Flow.dll"
#r "Streams.Core.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow

ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.thespian.worker.exe"

#time "on"

let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 4, logLevel = LogLevel.Debug)
cluster.AttachLogger(new ConsoleLogger())



let workers = cluster.Workers

cloud { return 42 } |> cluster.Run
cloud { return 42 } |> cluster.RunOnCurrentProcess

cluster.ShowProcesses()
cluster.ShowWorkers()
cluster.ClearSystemLogs()
cluster.ShowSystemLogs()

let cloudProcess = 
    CloudFlow.OfHttpFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> cluster.Submit

cloudProcess.Result


let test = cloud {
    let cell = ref 0
    let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { incr cell } ]
    return !cell
}

cluster.RunOnCurrentProcess(test, memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentProcess(test, memoryEmulation = MemoryEmulation.Copied)
cluster.Run test

let test' = cloud {
    return box(new System.IO.MemoryStream())
}

cluster.RunOnCurrentProcess(test', memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentProcess(test', memoryEmulation = MemoryEmulation.EnsureSerializable)
cluster.Run test'

let pflow =
    CloudFlow.OfArray [|1 .. 100|]
    |> CloudFlow.collect (fun i -> seq { for j in 1L .. 10000L -> int64 i * j })
    |> CloudFlow.filter (fun i -> i % 3L <> 0L)
    |> CloudFlow.map (fun i -> sprintf "Lorem ipsum dolor sit amet #%d" i)
    |> CloudFlow.cache
    |> cluster.Run

pflow |> Seq.length
pflow |> CloudFlow.length |> cluster.Run