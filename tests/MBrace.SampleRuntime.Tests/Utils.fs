namespace MBrace.SampleRuntime.Tests

open MBrace.SampleRuntime

type RuntimeSession(nodes : int) =
    
    static do MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

    let mutable runtime = None

    member __.Start () = 
        runtime <- Some <| MBraceRuntime.InitLocal(nodes)
        do System.Threading.Thread.Sleep 2000

    member __.Stop () =
        runtime |> Option.iter (fun r -> r.KillAllWorkers())
        runtime <- None

    member __.Runtime =
        match runtime with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r