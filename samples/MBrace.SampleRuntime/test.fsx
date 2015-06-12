#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"
#r "MBrace.Runtime.Core.dll"
#r "MBrace.Flow.dll"
#r "Streams.Core.dll"

open System
open MBrace.Core
open MBrace.Store
open MBrace.Workflows
open MBrace.SampleRuntime
open MBrace.Flow

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal 4
runtime.AttachLogger(new MBrace.Runtime.ConsoleSystemLogger())

runtime.Workers

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


[|0|] |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.toArray |> runtime.Run

runtime.Run (cloud { return System.AppDomain.CurrentDomain.GetAssemblies() |> Seq.groupBy (fun a -> a.FullName) |> Seq.map (fun (_,ids) -> ids |> Seq.map (fun id -> id.Location)|> Seq.toArray) |> Seq.filter (fun ids -> ids.Length > 1) |> Seq.toArray })