#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.SampleRuntime.exe"

open System
open Nessos.MBrace
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

runtime.Run(
    cloud {
        let! sp,rp = CloudChannel.New<int> ()
        let rec sender n = cloud {
            if n = 0 then return ()
            else
                do! CloudChannel.Send n sp
                return! sender (n-1)
        }

        let rec receiver n = cloud {
            if n = 100 then return ()
            else
                let! i = CloudChannel.Receive rp
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

type Cloud with
    static member All (one : IWorkerRef -> Cloud<'T>) : Cloud<'T []> =
        cloud {
            let! wr = Cloud.GetAvailableWorkers()
            let! handles = wr |> Array.map (fun w -> Cloud.StartChild(one w,w))
                              |> Cloud.Parallel
            return! handles |> Cloud.Parallel
        }

runtime.Run(Cloud.All (fun w -> cloud.Return w.Id))

runtime.Run(Cloud.All (fun _ -> Cloud.GetWorkerCount()))
