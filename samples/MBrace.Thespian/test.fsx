#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Thespian.exe"
#r "MBrace.Runtime.Core.dll"
#r "MBrace.Flow.dll"
#r "Streams.Core.dll"

open System
open MBrace.Core
open MBrace.Store
open MBrace.Workflows
open MBrace.Thespian
open MBrace.Flow

MBraceThespian.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Thespian.exe"

let runtime = MBraceThespian.InitLocal 4
runtime.AttachLogger(new ConsoleLogger())

let w = runtime.Workers

runtime.ShowProcessInfo()
runtime.ShowWorkerInfo()

let proc = 
    CloudFlow.OfHTTPFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> runtime.CreateProcess

proc.AwaitResult() |> Async.RunSynchronously

#time "on"

// vagabond data initialization test 1.
let c = ref 0
for i in 1 .. 10 do
    c := runtime.Run(cloud { return !c + 1 })

// vagabond data initialization test 2.
let mutable enabled = false

runtime.GetAllProcesses()

runtime.ClearAllProcesses()

runtime.GetWorkerInfo()

runtime.ShowProcessInfo()

runtime.Run(cloud { return enabled }, taskName = "test")

enabled <- true

runtime.Run(cloud { return enabled })

enabled <- false

runtime.Run(cloud { return enabled })

cloud {
    let client = new System.Net.WebClient()
    return client
} |> runtime.Run


let inputs = [|1L .. 1000000L|]
let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persist |> runtime.Run
let workers = Cloud.GetWorkerCount() |> runtime.Run
vector.PartitionCount 
vector.IsCachingEnabled
cloud { return! vector.ToEnumerable() } |> runtime.RunLocally
vector |> CloudFlow.sum |> runtime.RunLocally

let proc = runtime.CreateProcess(Cloud.Parallel [for i in 1 .. 10 -> Cloud.Sleep (i * 1000)], taskName = "test")

proc.ShowInfo()

runtime.ShowProcessInfo()

runtime.Run(Cloud.Parallel [for i in 1 .. 100 -> cloud { return i * i }])

#r "FsPIckler.dll"
open MBrace.Runtime.InMemoryRuntime

open Nessos.FsPickler

let runtime = InMemoryRuntime.Create(memoryMode = MemoryEmulation.Copied)

FsPickler.Clone (new MBrace.Runtime.InMemoryRuntime.InMemoryCancellationTokenSource())

runtime.Run <| Cloud.Parallel [for i in 1 .. 10 -> cloud { return i }]
