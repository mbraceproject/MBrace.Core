#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Thespian.exe"
#r "MBrace.Runtime.Core.dll"
#r "MBrace.Flow.dll"
#r "Streams.Core.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow

MBraceThespian.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Thespian.exe"

#time "on"

let cluster = MBraceThespian.InitLocal 4
cluster.AttachLogger(new ConsoleLogger())

let workers = cluster.Workers

cloud { return 42 } |> cluster.RunOnCloud
cloud { return 42 } |> cluster.RunOnCurrentProcess

cluster.ShowCloudTaskInfo()
cluster.ShowWorkerInfo()

let proc = 
    CloudFlow.OfHttpFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> cluster.CreateCloudTask

proc.AwaitResult() |> Async.RunSynchronously


let test = cloud {
    let cell = ref 0
    let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { incr cell } ]
    return !cell
}

cluster.RunOnCurrentProcess(test, memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentProcess(test, memoryEmulation = MemoryEmulation.Copied)
cluster.RunOnCloud test

let test' = cloud {
    return box(new System.IO.MemoryStream())
}

cluster.RunOnCurrentProcess(test', memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentProcess(test', memoryEmulation = MemoryEmulation.EnsureSerializable)
cluster.RunOnCloud test'

let pflow =
    CloudFlow.OfArray [|1 .. 100|]
    |> CloudFlow.collect (fun i -> seq { for j in 1L .. 10000L -> int64 i * j })
    |> CloudFlow.filter (fun i -> i % 3L <> 0L)
    |> CloudFlow.map (fun i -> sprintf "Lorem ipsum dolor sit amet #%d" i)
    |> CloudFlow.cache
    |> cluster.RunOnCloud

pflow |> Seq.length
pflow |> CloudFlow.length |> cluster.RunOnCloud

let task = Cloud.Parallel [for i in 1 .. 10 -> local { for j in 1 .. 10 do do! Cloud.Logf "Message %d,%d" i j }] |> Cloud.Ignore |> cluster.CreateCloudTask

cluster.ShowCloudLogs(task)