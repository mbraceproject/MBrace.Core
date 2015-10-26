namespace MBrace.Thespian.Tests

open System
open System.Threading

open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Thespian

type RuntimeSession(workerCount : int) =
    
    static do ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.thespian.worker.exe"
    static let e = ThespianWorker.LocalExecutable

    let lockObj = obj ()
    let mutable state : ThespianCluster option = None

    static member Init() = ignore e

    member __.Start () =
        lock lockObj (fun () -> 
            let runtime = ThespianCluster.InitOnCurrentMachine(workerCount, hostClusterStateOnCurrentProcess = true, logLevel = LogLevel.Debug)
            let _ = runtime.AttachLogger(new ConsoleLogger())
            while runtime.Workers.Length <> workerCount do Thread.Sleep 200
            state <- Some runtime)

    member __.Stop () =
        lock lockObj (fun () ->
            state |> Option.iter (fun r -> r.KillAllWorkers())
            state <- None)

    member __.Cluster =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r

    member __.Chaos() =
        lock lockObj (fun () ->
            let runtime = __.Cluster
            runtime.KillAllWorkers()
            while runtime.Workers.Length <> 0 do Thread.Sleep 200
            runtime.AttachNewLocalWorkers workerCount
            while runtime.Workers.Length <> workerCount do Thread.Sleep 200) 