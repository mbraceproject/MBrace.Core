#I "../../lib/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.SampleRuntime.exe"

open System
open MBrace
open MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../lib/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)


#I "../../bin"
#r "Streams.Core.dll"
#r "MBrace.Streams.dll"
#time "on"

open Nessos.Streams
open MBrace.Streams

let query1 = 
    runtime.Run (
        CloudStream.ofArray [|1 .. 1000|]
        |> CloudStream.flatMap(fun i -> [|1..10000|] |> Stream.ofArray |> Stream.map (fun j -> string i, j))
        |> CloudStream.toCloudArray)

let cached = runtime.Run <| CloudStream.cache(query1)

let query2 = runtime.Run (
                cached
                |> CloudStream.ofCloudArray
                |> CloudStream.sortBy snd 100
                |> CloudStream.toArray )

let query3 =
    runtime.Run(
        CloudStream.ofArray [|1 .. 1000|]
        |> CloudStream.flatMap(fun i -> [|1..10000|] |> Stream.ofArray |> Stream.map (fun j -> string i, j))
        |> CloudStream.sortBy snd 100
        |> CloudStream.toArray)

runtime.Run(
    cloud {
        let! workers = Cloud.GetAvailableWorkers()
        let! handles = 
            workers 
            |> Seq.map (fun w -> Cloud.StartChild(cloud { return printfn "%A" CloudArrayCache.State },w))
            |> Cloud.Parallel  
        return! handles |> Cloud.Parallel |> Cloud.Ignore              
    })

//VagrantRegistry.Initialize(throwOnError = false)
//let fsStore = FileSystemStore.LocalTemp :> ICloudFileStore
//let serializer = VagrantRegistry.Serializer
// 
//let array = Array.init (10) (fun i -> String.init 1024 (fun _ -> string i))
//let ca = CloudArray.CreateAsync(array, "foobar", fsStore, serializer, 1024L * 1024L)
//         |> Async.RunSync
//
//ca.Length
//ca.PartitionCount
//ca.GetPartition(0)
//ca.[7L]
//ca |> Seq.toArray