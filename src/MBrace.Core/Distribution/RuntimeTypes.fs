namespace MBrace.Core

open System
open System.Diagnostics


/// Specifies memory semantics in InMemory MBrace execution
type MemoryEmulation =
    /// Freely share values across MBrace workflows; async semantics.
    | Shared                = 1

    /// Freely share values across MBrace workflows but
    /// emulate serialization errors by checking that they are serializable.
    | EnsureSerializable    = 2

    /// Values copied when passed across worfklows; full distribution semantics.
    | Copied                = 4

/// Enumeration specifying the execution status for a CloudProcess
type CloudProcessStatus =
    /// Process posted to cluster for execution.
    | Created               = 1
    /// Process has been allocated and awaits execution.
    | WaitingToRun          = 2
    /// Process is being executed by the runtime.
    | Running               = 4
    /// Process encountered a runtime fault.
    | Faulted               = 8
    /// Process has completed successfully.
    | Completed             = 16
    /// Process has completed with an uncaught user exception.
    | UserException         = 32
    /// Process has been cancelled by the user.
    | Canceled              = 64


/// Denotes a reference to a worker node in the cluster.
type IWorkerRef =
    inherit IComparable
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string
    /// Worker processor count
    abstract ProcessorCount : int
    /// Gets the max CPU clock speed in MHz
    abstract MaxCpuClock : float
    /// Hostname of worker machine
    abstract Hostname : string
    /// Worker Process Id
    abstract ProcessId : int

/// Denotes a cloud process that is being executed in the cluster.
type ICloudProcess =
    /// Unique cloud process identifier
    abstract Id : string
    /// Gets the cancellation corresponding to the Task instance
    abstract CancellationToken : ICloudCancellationToken
    /// Gets a TaskStatus enumeration indicating the current cloud process state.
    abstract Status : CloudProcessStatus
    /// Gets a boolean indicating that the cloud process has completed successfully.
    abstract IsCompleted : bool
    /// Gets a boolean indicating that the cloud process has completed with fault.
    abstract IsFaulted : bool
    /// Gets a boolean indicating that the cloud process has been canceled.
    abstract IsCanceled : bool
    /// Synchronously gets the cloud process result, blocking until it completes.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract ResultBoxed : obj
    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResultBoxed : ?timeoutMilliseconds:int -> Async<obj>
    /// Returns the cloud process result if completed or None if still pending.
    abstract TryGetResultBoxed : unit -> Async<obj option>

/// Denotes a cloud process that is being executed in the cluster.
type ICloudProcess<'T> =
    inherit ICloudProcess
    /// <summary>
    ///     Awaits cloud process for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResult : ?timeoutMilliseconds:int -> Async<'T>
    /// Returns the cloud process result if completed or None if still pending.
    abstract TryGetResult : unit -> Async<'T option>
    /// Synchronously gets the cloud process result, blocking until it completes.
    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    abstract Result : 'T


namespace MBrace.Core.Internals

open System
open MBrace.Core

module WorkerRef =

    /// <summary>
    ///     Partitions a set of inputs to workers.
    /// </summary>
    /// <param name="workers">Workers to be partition work to.</param>
    /// <param name="inputs">Input work.</param>
    let partition (workers : IWorkerRef []) (inputs: 'T[]) : (IWorkerRef * 'T []) [] =
        if workers = null || workers.Length = 0 then invalidArg "workers" "must be non-empty."
        inputs
        |> Array.splitByPartitionCount workers.Length
        |> Seq.mapi (fun i p -> (workers.[i],p))
        |> Seq.filter (fun (_,p) -> not <| Array.isEmpty p)
        |> Seq.toArray
    
    /// <summary>
    ///     Partitions a set of inputs according to a weighted set of workers.
    /// </summary>
    /// <param name="weight">Weight function.</param>
    /// <param name="workers">Input workers.</param>
    /// <param name="inputs">Input array to be partitioned.</param>
    let partitionWeighted (weight : IWorkerRef -> int) (workers : IWorkerRef []) (inputs: 'T[]) : (IWorkerRef * 'T []) [] =
        if workers = null then raise <| new ArgumentNullException("workers")
        let weights = workers |> Array.map weight
        inputs
        |> Array.splitWeighted weights
        |> Seq.mapi (fun i p -> (workers.[i],p))
        |> Seq.filter (fun (_,p) -> not <| Array.isEmpty p)
        |> Seq.toArray