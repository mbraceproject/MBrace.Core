﻿namespace MBrace.Core

open System
open System.Threading
open System.Threading.Tasks

/// Denotes a reference to a worker node in the cluster.
type IWorkerRef =
    inherit IComparable
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string
    /// Worker processor count
    abstract ProcessorCount : int

module WorkerRef =

    /// partitions a set of inputs to workers
    let partition (workers : IWorkerRef []) (inputs: 'T[]) : (IWorkerRef * 'T []) [] =
        if workers = null || workers.Length = 0 then invalidArg "workers" "must be non-empty."
        inputs
        |> Array.splitByPartitionCount workers.Length
        |> Seq.mapi (fun i p -> (workers.[i],p))
        |> Seq.filter (fun (_,p) -> not <| Array.isEmpty p)
        |> Seq.toArray
    
    /// partitions a set of inputs to workers -- weighted by worker processor count.
    let partitionWeighted (workers : IWorkerRef []) (inputs: 'T[]) : (IWorkerRef * 'T []) [] =
        if workers = null || workers.Length = 0 then invalidArg "workers" "must be non-empty."
        let weights = workers |> Array.map (fun w -> w.ProcessorCount)
        inputs
        |> Array.splitWeighted weights
        |> Seq.mapi (fun i p -> (workers.[i],p))
        |> Seq.filter (fun (_,p) -> not <| Array.isEmpty p)
        |> Seq.toArray

/// Denotes a task that is being executed in the cluster.
type ICloudTask<'T> =
    /// Unique task identifier
    abstract Id : string
    /// Gets a TaskStatus enumeration indicating the current task state.
    abstract Status : TaskStatus
    /// Gets a boolean indicating that the task has completed successfully.
    abstract IsCompleted : bool
    /// Gets a boolean indicating that the task has completed with fault.
    abstract IsFaulted : bool
    /// Gets a boolean indicating that the task has been canceled.
    abstract IsCanceled : bool
    /// Awaits task for completion, returning its eventual result
    abstract AwaitResult : ?timeoutMilliseconds:int -> Local<'T>
    /// Rreturns the task result if completed or None if still pending.
    abstract TryGetResult : unit -> Local<'T option>
    /// Synchronously gets the task result, blocking until it completes.
    abstract Result : 'T