namespace MBrace.Runtime

open System
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals

[<Sealed; DataContract>]
type WorkerRef internal (id : WorkerId, wmon : IWorkerManager) =
    let cvalue = CacheAtom.Create(async { return! runtime.WorkerManager.TryGetWorkerInfo wref }, intervalMilliseconds = 100, keepLastResultOnError = true)
    do cvalue.Force() |> ignore
    let getState () =
        match cvalue.Value with
        | None -> invalidOp <| sprintf "Worker '%s' is no longer part of runtime '%s'." wref.Id runtime.Id
        | Some cv -> cv

    member __.WorkerRef = wref
    member __.CpuUsage = getState().PerformanceMetrics.CpuUsage
    member __.ProcessorCount = wref.ProcessorCount
    member __.MemoryUsage = getState().PerformanceMetrics.MemoryUsage
    member __.TotalMemory = getState().PerformanceMetrics.TotalMemory
    member __.ActiveJobs = getState().State.CurrentJobCount
    member __.MaxJobCount = getState().State.MaxJobCount
    member __.NetworkUsageUp = getState().PerformanceMetrics.NetworkUsageUp
    member __.NetworkUsageDown = getState().PerformanceMetrics.NetworkUsageDown
    member __.Hostname = wref.Hostname
    member __.LastHeartbeat = getState().LastHeartbeat
    member __.InitializationTime = getState().InitializationTime
    member __.Status = getState().State.Status

    member w.GetInfo() : string = WorkerReporter.Report([|w|], "Worker", borders = false)
    member w.ShowInfo () : unit = Console.WriteLine(w.GetInfo())