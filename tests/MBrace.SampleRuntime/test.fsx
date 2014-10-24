#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"

open Nessos.MBrace
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

runtime.Run (
    cloud {
        do! Cloud.Sleep 5000
        return! Array.init 100 (fun i -> cloud { return printfn "hi" ; return i }) |> Cloud.Parallel
    })

runtime.Run (
    cloud {
        do! Cloud.Sleep 5000
        return! Array.init 100 (fun i -> cloud { return if i = 78 then failwith "error" else printfn "hi" ; i }) |> Cloud.Parallel
    })


let test = cloud {
    use foo = { new ICloudDisposable with member __.Dispose () = async { return printfn "disposed" } }
    return! cloud { return foo.GetHashCode () } <||> cloud { return foo.GetHashCode () }
}

runtime.Run test