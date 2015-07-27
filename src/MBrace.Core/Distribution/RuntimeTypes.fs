namespace MBrace.Core

open System
open System.Threading
open System.Threading.Tasks

/// Specifies memory semantics in InMemory MBrace execution
type MemoryEmulation =
    /// Freely share values across MBrace workflows; async semantics.
    | Shared                = 0

    /// Freely share values across MBrace workflows but
    /// emulate serialization errors by checking that they are serializable.
    | EnsureSerializable    = 1

    /// Values copied when passed across worfklows; full distributed semantics.
    | Copied                = 2


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

type ICloudTask =
    /// Unique task identifier
    abstract Id : string
    /// Gets the cancellation corresponding to the Task instance
    abstract CancellationToken : ICloudCancellationToken
    /// Gets a TaskStatus enumeration indicating the current task state.
    abstract Status : TaskStatus
    /// Gets a boolean indicating that the task has completed successfully.
    abstract IsCompleted : bool
    /// Gets a boolean indicating that the task has completed with fault.
    abstract IsFaulted : bool
    /// Gets a boolean indicating that the task has been canceled.
    abstract IsCanceled : bool
    /// Synchronously gets the task result, blocking until it completes.
    abstract ResultBoxed : obj
    /// <summary>
    ///     Awaits task for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResultBoxed : ?timeoutMilliseconds:int -> Async<obj>
    /// Returns the task result if completed or None if still pending.
    abstract TryGetResultBoxed : unit -> Async<obj option>

/// Denotes a task that is being executed in the cluster.
type ICloudTask<'T> =
    inherit ICloudTask
    /// <summary>
    ///     Awaits task for completion, returning its eventual result.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Default to infinite timeout.</param>
    abstract AwaitResult : ?timeoutMilliseconds:int -> Async<'T>
    /// Returns the task result if completed or None if still pending.
    abstract TryGetResult : unit -> Async<'T option>
    /// Synchronously gets the task result, blocking until it completes.
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