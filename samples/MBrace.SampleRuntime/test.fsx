#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.SampleRuntime.exe"

open System
open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

open Nessos.MBrace.Channels

runtime.Run(
    cloud {
        let! chan = Channel.New<int> ()
        let rec sender n = cloud {
            if n = 0 then return ()
            else
                do! Channel.Send n chan
                return! sender (n-1)
        }

        let rec receiver n = cloud {
            if n = 100 then return ()
            else
                let! i = Channel.Receive chan
                printfn "RECEIVED : %d" i
                return! receiver (n + 1)
        }

        let! _ = sender 100 <||> receiver 0
        return ()
    })

let getWordCount inputSize =
    let map (text : string) = cloud { return text.Split(' ').Length }
    let reduce i i' = cloud { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    MapReduce.mapReduce map 0 reduce inputs