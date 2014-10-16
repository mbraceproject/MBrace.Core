#r "../bin/MBrace.Cloud.dll"
#r "../bin/MBrace.Cloud.Parallel.dll"

open Nessos.MBrace


let verbose = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return i }) |> Cloud.Parallel
let failed = Array.init 100 (fun i -> cloud { if i = 55 then return failwith "boom!" else printfn "task %d" i }) |> Cloud.Parallel


Cloud.RunLocal verbose
Cloud.RunLocal failed

//Cloud.RunWithScheduler(Cloud.ToSequential verbose)
//Cloud.RunWithScheduler(Cloud.ToSequential failed)


let none = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return Option<int>.None }) |> Cloud.Choice
let some = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return if i = 0 then Some i else None }) |> Cloud.Choice
let exn = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return if i = 55 then failwith "boom!" else Option<int>.None }) |> Cloud.Choice

Cloud.RunLocal none
Cloud.RunLocal some
Cloud.RunLocal exn

//Cloud.RunWithScheduler(Cloud.ToSequential none)
//Cloud.RunWithScheduler(Cloud.ToSequential some)
//Cloud.RunWithScheduler(Cloud.ToSequential exn)
