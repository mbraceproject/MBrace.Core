namespace MBrace.SampleRuntime.Tests

open MBrace.Tests
open MBrace.SampleRuntime

type LogTester(runtime : MBraceRuntime) =
    let logs = new ResizeArray<string>()
    let d = runtime.Logs.Subscribe(logs.Add)

    interface ILogTester with
        member __.GetLogs() = logs.ToArray()
        member __.Clear() = lock logs logs.Clear

    member __.Dispose() = d.Dispose()

type RuntimeSession(nodes : int) =
    
    static do MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

    let mutable state = None

    member __.Start () = 
        let runtime = MBraceRuntime.InitLocal(nodes)
        let logger = new LogTester(runtime)
        state <- Some(runtime, logger)
        do System.Threading.Thread.Sleep 2000

    member __.Stop () =
        state |> Option.iter (fun (r,d) -> r.KillAllWorkers() ; d.Dispose())
        state <- None

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (r,_) -> r

    member __.Logger =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some (_,l) -> l