namespace MBrace.SampleRuntime.Tests

open System
open System.Threading

open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.SampleRuntime

type LogTester() =
    let logs = new ResizeArray<string>()

    interface ISystemLogger with
        member __.LogEntry(_,_,m) = logs.Add m

    interface ILogTester with
        member __.GetLogs() = logs.ToArray()
        member __.Clear() = lock logs logs.Clear

type RuntimeSession(nodes : int) =
    
    static do MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

    let lockObj = obj ()
    let mutable state : (MBraceRuntime * LogTester) option = None

    member __.Start () =
        lock lockObj (fun () -> 
            let runtime = MBraceRuntime.InitLocal(nodes)
            let logger = new LogTester()
            let _ = runtime.AttachLogger logger
            while runtime.Workers.Length <> nodes do Thread.Sleep 200
            state <- Some(runtime, logger))

    member __.Stop () =
        lock lockObj (fun () ->
            state |> Option.iter (fun (r,d) -> r.KillAllWorkers())
            state <- None)

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (r,_) -> r

    member __.Logger =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (_,l) -> l

    member __.Chaos() =
        lock lockObj (fun () ->
            let runtime = __.Runtime
            runtime.KillAllWorkers()
            while runtime.Workers.Length <> 0 do Thread.Sleep 200
            runtime.AppendWorkers nodes
            while runtime.Workers.Length <> nodes do Thread.Sleep 200) 