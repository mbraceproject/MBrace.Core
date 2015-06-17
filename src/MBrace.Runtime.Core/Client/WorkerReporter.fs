namespace MBrace.Runtime

open System
open System.Collections.Concurrent

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Utils.PerformanceMonitor

[<AutoSerializable(false); Sealed>]
type Worker internal (wref : IWorkerRef, runtime : IRuntimeManager) =
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
        


and [<AutoSerializable(false)>] internal 
  WorkerManager(runtime : IRuntimeManager) =
    
    // TODO : add cleanup logic
    let workers = new ConcurrentDictionary<IWorkerRef, Worker> ()

    /// <summary>
    ///     Fetches worker by provided id.
    /// </summary>
    /// <param name="taskId">Task identifier.</param>
    member wm.GetWorkerById (workerRef : IWorkerRef) = async {
        let ok,w = workers.TryGetValue workerRef
        if ok then return w
        else
            return workers.GetOrAdd(workerRef, fun w -> new Worker(w, runtime))
    }

    /// <summary>
    ///     Gets all workers running in the cluster.
    /// </summary>
    member wm.GetAllWorkers() = async {
        let! workers = runtime.WorkerManager.GetAvailableWorkers()
        return! 
            workers
            |> Seq.map (fun w -> w.WorkerRef) 
            |> Seq.distinct
            |> Seq.map wm.GetWorkerById
            |> Async.Parallel
    }

    member wm.GetInfo () : string = 
        let workers = wm.GetAllWorkers() |> Async.RunSync
        WorkerReporter.Report(workers, "Workers", borders = false)

    member wm.ShowInfo() : unit = Console.WriteLine(wm.GetInfo())

and internal WorkerReporter private () =
    
    static let template : Field<Worker> list = 
        let inline ( *?) x (y : Nullable<_>) =
            if y.HasValue then new Nullable<_>(x * y.Value)
            else new Nullable<_>()

        let inline (?/?) (x : Nullable<_>) (y : Nullable<_>) =
            if x.HasValue && y.HasValue then new Nullable<_>(x.Value / y.Value)
            else new Nullable<_>()

        let double_printer (value : Nullable<double>) = 
            if value.HasValue then sprintf "%.1f" value.Value
            else "N/A"

        [ Field.create "Id" Left (fun w -> w.WorkerRef.Id)
          Field.create "Status" Left (fun p -> string p.Status)
          Field.create "% CPU / Cores" Center (fun p -> sprintf "%s / %d" (double_printer p.CpuUsage) p.WorkerRef.ProcessorCount)
          Field.create "% Memory / Total(MB)" Center (fun p ->
                let memPerc = 100. *? p.MemoryUsage ?/? p.TotalMemory |> double_printer
                sprintf "%s / %s" memPerc <| double_printer p.TotalMemory
            )
          Field.create "Network(ul/dl : KB/s)" Center (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUsageUp <| double_printer n.NetworkUsageDown)
          Field.create "Jobs" Center (fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobCount)
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "Process Id" Right (fun p -> p.WorkerRef.ProcessId)
          Field.create "Heartbeat" Left (fun p -> p.LastHeartbeat)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
        ]
    
    static member Report(workers : seq<Worker>, title : string, borders : bool) = 
        let ws = workers
                 |> Seq.sortBy (fun w -> w.InitializationTime)
                 |> Seq.toList

        Record.PrettyPrint(template, ws, title, borders)
