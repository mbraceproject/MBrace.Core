namespace MBrace.SampleRuntime.Tests

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

    let mutable state = None

    member __.Start () = 
        let runtime = MBraceRuntime.InitLocal(nodes)
        let logger = new LogTester()
        let _ = runtime.AttachLogger logger
        state <- Some(runtime, logger)
        do System.Threading.Thread.Sleep 2000

    member __.Stop () =
        state |> Option.iter (fun (r,d) -> r.KillAllWorkers())
        state <- None

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (r,_) -> r

    member __.Logger =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (_,l) -> l