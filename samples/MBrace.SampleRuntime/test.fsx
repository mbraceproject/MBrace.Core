#I "../../bin/"

#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.SampleRuntime.exe"

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.SampleRuntime

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.SampleRuntime.exe"

let runtime = MBraceRuntime.InitLocal(4)

let getWordCount inputSize =
    let map (text : string) = cloud { return text.Split(' ').Length }
    let reduce i i' = cloud { return i + i' }
    let inputs = Array.init inputSize (fun i -> "lorem ipsum dolor sit amet")
    MapReduce.mapReduce map 0 reduce inputs


let t = runtime.RunAsTask(getWordCount 2000)
do System.Threading.Thread.Sleep 3000
runtime.KillAllWorkers() 
runtime.AppendWorkers 4

t.Result

let testFunc = cloud {
    let! result = Array.init 20 (fun i -> cloud { return if i = 15 then failwith "kaboom!" else i }) |> Cloud.Parallel
    return Array.sum result
}

let rec fib n = cloud {
    try
        if n <= 1 then return failwith "kaboom"
        else
            let! fnn, fn = fib (n-2) <||> fib (n-1)
            return fnn + fn
    with e ->
        return! Cloud.Raise e
}

runtime.Run (fib 10)
runtime.Run testFunc


let rec dive n e =
    if n = 0 then raise e
    else
        1 + dive (n-1) e

let e = new System.Exception()

let exns = Array.init 100 (fun i -> async { return try dive 2 e ; failwith "" with e -> e }) |> Async.Parallel |> Async.RunSynchronously

e.StackTrace

exns |> Array.forall (fun e' -> obj.ReferenceEquals(e,e'))


open System.Reflection
open System.Runtime.Serialization
open System.Runtime.ExceptionServices

let clone (e : #exn) =
    let bf = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
    use m = new System.IO.MemoryStream()
    bf.Serialize(m, e)
    m.Position <- 0L
    bf.Deserialize m :?> exn

#r "FsPickler.dll"

open Nessos.FsPickler

// resolve the internal stacktrace field in exception
// this is implementation-sensitive so not guaranteed to work. 
let remoteStackTraceField : FieldInfo =
    let bfs = BindingFlags.NonPublic ||| BindingFlags.Instance
    match typeof<System.Exception>.GetField("remote_stack_trace", bfs) with
    | null -> typeof<System.Exception>.GetField("_remoteStackTraceString", bfs)
    | f -> f

let trySetRemoteStackTraceField (trace : string) (e : exn) =
    match remoteStackTraceField with
    | null -> false
    | f -> f.SetValue(e, trace) ; true

let tryGetRemoteStackTraceField (e : exn) =
    match remoteStackTraceField with
    | null -> None
    | f -> Some (f.GetValue e :?> string)


let lsObj (t:'T) =
    let fs = typeof<'T>.GetFields(BindingFlags.NonPublic ||| BindingFlags.Instance) |> Array.sortBy(fun f -> f.Name)
    for f in fs do
        printfn "#### %s\t %A" f.Name <| f.GetValue(t)


let getStacked e = try dive 5 e ; failwith "" with e -> e

let e = getStacked (new System.Exception("mple"))

let e' = clone e

do raise e

lsObj e
lsObj e'

e'.StackTrace
e.StackTrace

trySetRemoteStackTraceField "   at gamo to kerato mou\n" e

e.StackTrace

let e' = clone e

e'

let edi = ExceptionDispatchInfo.Capture e'

edi.SourceException

edi.Throw()