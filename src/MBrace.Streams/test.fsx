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
#r "MBrace.Streams.dll"
#time "on"

open Nessos.Streams
open MBrace.Streams

let query1 = 
    runtime.Run (
        CloudStream.ofArray [|1 .. 1000|]
        |> CloudStream.flatMap(fun i -> [|1..10000|] |> Stream.ofArray |> Stream.map (fun j -> string i, j))
        |> CloudStream.toCloudVector)


runtime.Run <| CloudStream.cache(query1)

query1.CacheMap.Value |> runtime.RunLocal

runtime.RunLocal(query1.ToEnumerable())
|> Seq.toArray

let query2 = runtime.Run (
                query1
                |> CloudStream.ofCloudVector
                |> CloudStream.sortBy snd 100
                |> CloudStream.toArray )

let query3 =
    runtime.Run(
        CloudStream.ofArray [|1 .. 1000|]
        |> CloudStream.flatMap(fun i -> [|1..10000|] |> Stream.ofArray |> Stream.map (fun j -> string i, j))
        |> CloudStream.sortBy snd 100
        |> CloudStream.toArray)



