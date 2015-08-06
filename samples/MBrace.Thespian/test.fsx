#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Thespian.exe"
#r "MBrace.Runtime.Core.dll"
#r "MBrace.Flow.dll"
#r "Streams.Core.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow

MBraceThespian.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Thespian.exe"

#time "on"

let cluster = MBraceThespian.InitLocal 4
cluster.AttachLogger(new ConsoleLogger())

let workers = cluster.Workers

cloud { return 42 } |> cluster.Run

cluster.ShowProcessInfo()
cluster.ShowWorkerInfo()

let proc = 
    CloudFlow.OfHttpFileByLine "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt"
    |> CloudFlow.length
    |> cluster.CreateProcess

proc.AwaitResult() |> Async.RunSynchronously


let test = cloud {
    let cell = ref 0
    let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { incr cell } ]
    return !cell
}

cluster.RunOnCurrentMachine(test, memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentMachine(test, memoryEmulation = MemoryEmulation.Copied)
cluster.Run test

let test' = cloud {
    return box(new System.IO.MemoryStream())
}

cluster.RunOnCurrentMachine(test', memoryEmulation = MemoryEmulation.Shared)
cluster.RunOnCurrentMachine(test', memoryEmulation = MemoryEmulation.EnsureSerializable)
cluster.Run test'

let pflow =
    CloudFlow.OfArray [|1 .. 100|]
    |> CloudFlow.collect (fun i -> seq { for j in 1L .. 10000L -> int64 i * j })
    |> CloudFlow.filter (fun i -> i % 3L <> 0L)
    |> CloudFlow.map (fun i -> sprintf "Lorem ipsum dolor sit amet #%d" i)
    |> CloudFlow.cache
    |> cluster.Run

pflow |> Seq.length
pflow |> CloudFlow.length |> cluster.Run


let large = [|1L .. 10000000L|]

let large' = [for i in 1L .. 1000000L -> sprintf "value value value %d" i ]

let test x = cloud {
    return! Cloud.Parallel [for i in 1 .. 10 -> cloud { return x.GetHashCode() }]
}

let test' Ν = 
    let x = [|1L .. Ν|]
    let y = [|x;x;x;x|]
    cloud {
        return! Cloud.Parallel [for i in 1 .. 10 -> cloud { return y.GetHashCode() }]
    }

cluster.Run(test' 10000001L)

cluster.Run(test [|large'|])


let test0 () =
    let large = [|1L .. 10000002L|]
    cloud {
        let! workerCount = Cloud.GetWorkerCount()
        let! hashCodes = Cloud.Parallel [for i in 1  .. 100 -> cloud { return large.GetHashCode() } ]
        return
            hashCodes 
            |> Seq.distinct 
            |> Seq.length

    } |> cluster.Run

test0()

#r "FsPickler.dll"
open Nessos.FsPickler


let small = [|1 .. 1000000|]




let test () =
    let large = [|for i in 1 .. 99 -> [|1 .. 1000000|]|]
    cluster.Run (cloud { return large.GetHashCode() })


let test () =

    let getRefHashCode (t : 'T when 'T : not struct) = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode t

    let large = [for i in 1 .. 1000000 -> seq { for j in 0 .. i % 7 -> "lorem ipsum"} |> String.concat "-"]
    cloud {
        let! workerCount = Cloud.GetWorkerCount()
        let! hashCodes = Cloud.Parallel [for i in 1  .. (3 * workerCount) -> cloud { return getRefHashCode large } ]

        return
            hashCodes 
            |> Seq.distinct 
            |> Seq.sort
            |> Seq.toArray

    } |> cluster.Run

test()