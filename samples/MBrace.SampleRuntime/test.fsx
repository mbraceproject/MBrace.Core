#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"

open System
open MBrace
open MBrace.Workflows
open MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

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
    let map (text : string) = cloud { return text.Split(' ').Length }
    let reduce i i' = cloud { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    Distributed.mapReduce map reduce 0 inputs

runtime.Run (getWordCount 1000)

runtime.KillAllWorkers()
runtime.AppendWorkers 4

let t1 = runtime.RunAsTask(Cloud.Sleep 20000, faultPolicy = FaultPolicy.NoRetry)
let t2 = runtime.RunAsTask(Cloud.Sleep 20000)
let t3 = runtime.RunAsTask(Cloud.WithFaultPolicy FaultPolicy.NoRetry (Cloud.Sleep 20000 <||> Cloud.Sleep 20000))

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


let foo = cloud {
    let! x = Cloud.ToLocal(cloud { return 42})
    return! Seq.init 1000 (fun i -> cloud { return i}) |> Cloud.Parallel
}

runtime.Run foo