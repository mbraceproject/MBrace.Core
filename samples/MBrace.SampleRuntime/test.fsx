#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"

open Nessos.MBrace
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

let successful = cloud {
//    do! Cloud.Sleep 5000
    return! Array.init 1000 (fun i -> cloud { return printfn "hi" ; return i }) |> Cloud.Parallel
}

let failed = cloud {
    do! Cloud.Sleep 5000
    return! Array.init 100 (fun i -> cloud { return if i = 78 then failwith "error" else printfn "hi" ; i }) |> Cloud.Parallel
}


let t = runtime.RunAsTask successful
do System.Threading.Thread.Sleep 3000
runtime.KillAllWorkers() 
runtime.AppendWorkers 4

t.Result