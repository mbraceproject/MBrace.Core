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

cloud { return 42 } |> cluster.Run

cluster.ShowProcessInfo()
cluster.ShowWorkerInfo()

let proc = 
    CloudFlow.OfHttpFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> cluster.CreateProcess

proc.AwaitResult() |> Async.RunSynchronously


let test = cloud {
    let cell = ref 0
    let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { incr cell } ]
    return !cell
}

cluster.RunOnCurrentMachine(test, memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentMachine(test, memoryEmulation = MemoryEmulation.Copied)
cluster.Run test

let test' = cloud {
    return box(new System.IO.MemoryStream())
}

cluster.RunOnCurrentMachine(test', memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentMachine(test', memoryEmulation = MemoryEmulation.EnsureSerializable)
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


let large = [|1L .. 10000000L|]

let test (ts : 'T  []) = cloud {
    let! workerCount = Cloud.GetWorkerCount()
    // warmup; ensure cached everywhere before sending actual test
    do! Cloud.ParallelEverywhere(cloud { return ts.GetHashCode() }) |> Cloud.Ignore
    let! hashCodes = Cloud.Parallel [for i in 1 .. 5 * workerCount -> cloud { return ts.GetHashCode() }]
    let uniqueHashes =
        hashCodes
        |> Seq.distinct
        |> Seq.length

    return workerCount = uniqueHashes
}

cluster.Run(cloud { return 42})

let y = test large in cluster.Run y


let foo (ts : 'T []) = cloud {
    let pair = [1;2] |> List.map (fun _ -> ts)
    let! t = Cloud.StartAsTask(cloud { return obj.ReferenceEquals(pair.[0], pair.[1])})
    return! t.AwaitResult()
}

foo large |> cluster.Run