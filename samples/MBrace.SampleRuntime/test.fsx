#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.SampleRuntime.exe"

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

let getWordCount inputSize =
    let map (text : string) = cloud { return text.Split(' ').Length }
    let reduce i i' = cloud { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    MapReduce.mapReduce map 0 reduce inputs


let t = runtime.RunAsTask(getWordCount 2000)
do System.Threading.Thread.Sleep 3000
runtime.KillAllWorkers() 
runtime.AppendWorkers 4

t.Result

let testFunc = cloud {
    let! result = Array.init 20 (fun i -> cloud { return if i = 15 then failwith "kaboom!" else i }) |> Cloud.Parallel
    return Array.sum result
}

let rec fib n = cloud {
    if n <= 1 then return failwith "kaboom"
    else
        let! fnn, fn = fib (n-2) <||> fib (n-1)
        return fnn + fn
}

let test = cloud {
    try return! fib 10
    with e ->
        System.Console.WriteLine("{0}", e)
        return! Cloud.Raise e
//        return raise e // using raise should clear stacktrace
}

runtime.Run test