namespace MBrace.Thespian.Tests

open System
open System.Threading

open MBrace.Core.Tests
open MBrace.Runtime
open MBrace.Thespian

type RuntimeSession(nodes : int) =
    
    static do MBraceThespian.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.Thespian.exe"

    let lockObj = obj ()
    let mutable state : MBraceThespian option = None

    member __.Start () =
        lock lockObj (fun () -> 
            let runtime = MBraceThespian.InitLocal(nodes, logLevel = LogLevel.Debug)
            let _ = runtime.AttachLogger(new ConsoleLogger())
            while runtime.Workers.Length <> nodes do Thread.Sleep 200
            state <- Some runtime)

    member __.Stop () =
        lock lockObj (fun () ->
            state |> Option.iter (fun r -> r.KillAllWorkers())
            state <- None)

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r

    member __.Chaos() =
        lock lockObj (fun () ->
            let runtime = __.Runtime
            runtime.KillAllWorkers()
            while runtime.Workers.Length <> 0 do Thread.Sleep 200
            runtime.AppendWorkers nodes
            while runtime.Workers.Length <> nodes do Thread.Sleep 200) 

//type LogTester(session : RuntimeSession) =
//    let logs = new ResizeArray<string>()
//
//    interface ISystemLogger with
//        member l.LogEntry(_,_,m) = logs.Add m
//
//    interface ILogTester with
//        member l.GetLogs() = logs.ToArray()
//        member l.Init() =
//            let d = session.Runtime.AttachLogger l
//            { new IDisposable with member __.Dispose() = d.Dispose() ; logs.Clear() }