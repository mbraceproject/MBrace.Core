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

let fileStore = MBrace.Runtime.Store.FileSystemStore.Create("D:\\wiki")



let runtime = MBraceRuntime.InitLocal(4, fileStore = fileStore)




let files = runtime.StoreClient.File.Enumerate("")
let paths = files |> Array.map (fun file -> file.Path) |> Array.filter (fun path -> not <| path.EndsWith("fool.txt"))

files.[0].Size |> runtime.RunLocally


runtime.Run <| cloud { return 42 }

#time "on"

let result = 
    CloudFlow.OfCloudFiles(paths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length })) 
    |> CloudFlow.length
    |> runtime.Run







runtime.Run(
    cloud {
        let! sp,rp = CloudChannel.New<int> ()
        let rec sender n = cloud {
            if n = 0 then return ()
            else
                do! CloudChannel.Send (sp, n)
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
    let map (text : string) = local { return text.Split(' ').Length }
    let reduce i i' = local { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    DivideAndConquer.mapReduce map reduce 0 inputs

runtime.Run (getWordCount 1000)

runtime.KillAllWorkers()
runtime.AppendWorkers 4

let t1 = runtime.StartAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
let t2 = runtime.StartAsTask(Cloud.Sleep 20000)
let t3 = runtime.StartAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))

t1.Result

let rec stackOverflow () = 1 + stackOverflow()

let rec test () = cloud {
    try
        let! wf = Cloud.StartChild(cloud { return stackOverflow() })
        return! wf
    with _ -> 
        return! test ()
//        let! wf = Cloud.StartChild(test ())
//        return! wf
}

runtime.Run(test(), faultPolicy = FaultPolicy.NoRetry)


Cloud.Parallel [cloud { return 432 } ; local { return 1 } :> _ ]

local {
    let! x = Cloud.Parallel [ cloud { return 42 } ]
    return x
}

// vagabond data initialization test 1.
let c = ref 0
for i in 1 .. 10 do
    c := runtime.Run(cloud { return !c + 1 })

// vagabond data initialization test 2.
let mutable enabled = false

runtime.Run(cloud { return enabled })

enabled <- true

runtime.Run(cloud { return enabled })

enabled <- false

runtime.Run(cloud { return enabled })