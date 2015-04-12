#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"

open System
open MBrace
open MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)


#I "../../bin"
#r "Streams.Core.dll"
#r "MBrace.Flow.dll"
#time "on"

open Nessos.Streams
open MBrace.Flow

for i in 1..100 do
    let source = [| 1..10 |]
    let q = source |> CloudFlow.ofArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray
    printfn "%A" <| runtime.Run q

let query1 = 
    runtime.Run (
        CloudFlow.ofArray [|1 .. 1000|]
        |> CloudFlow.collect(fun i -> [|1..10000|] |> Seq.map (fun j -> string i, j))
        |> CloudFlow.toCloudVector)


runtime.Run <| CloudFlow.cache(query1)

query1.CacheMap.Value |> runtime.RunLocal

runtime.RunLocal(query1.ToEnumerable())
|> Seq.toArray

let query2 = runtime.Run (
                query1
                |> CloudFlow.OfCloudVector
                |> CloudFlow.sortBy snd 100
                |> CloudFlow.toArray )

let query3 =
    runtime.Run(
        CloudFlow.ofArray [|1 .. 1000|]
        |> CloudFlow.collect(fun i -> [|1..10000|] |> Seq.map (fun j -> string i, j))
        |> CloudFlow.sortBy snd 100
        |> CloudFlow.toArray)