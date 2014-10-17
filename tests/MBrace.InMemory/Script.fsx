#I "../../bin/"

#r "MBrace.Cloud.dll"
#r "MBrace.InMemory.dll"

open Nessos.MBrace
open Nessos.MBrace.InMemory


let verbose = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return i }) |> Cloud.Parallel
let failed = Array.init 100 (fun i -> cloud { if i = 55 then return failwith "boom!" else printfn "task %d" i }) |> Cloud.Parallel


Cloud.RunSynchronously(verbose, resources = InMemory.Resource)
Cloud.RunSynchronously(failed, resources = InMemory.Resource)

Cloud.RunSynchronously(Cloud.ToSequential verbose, resources = InMemory.Resource)
Cloud.RunSynchronously(Cloud.ToSequential failed, resources = InMemory.Resource)


let none = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return Option<int>.None }) |> Cloud.Choice
let some = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return if i = 0 then Some i else None }) |> Cloud.Choice
let exn = Array.init 100 (fun i -> cloud { printfn "task %d" i ; return if i = 55 then failwith "boom!" else Option<int>.None }) |> Cloud.Choice

Cloud.RunSynchronously(none, resources = InMemory.Resource)
Cloud.RunSynchronously(some, resources = InMemory.Resource)
Cloud.RunSynchronously(exn,  resources = InMemory.Resource)

Cloud.RunSynchronously(Cloud.ToSequential none, resources = InMemory.Resource)
Cloud.RunSynchronously(Cloud.ToSequential some, resources = InMemory.Resource)
Cloud.RunSynchronously(Cloud.ToSequential exn, resources = InMemory.Resource)

Cloud.RunSynchronously(CloudRef.New [| 1 .. 10000 |], resources = InMemory.Resource)