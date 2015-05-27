namespace MBrace.Runtime

open System

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

open MBrace.Runtime.Utils

#nowarn "444"

type ICloudCounter =
    abstract Increment : unit -> Async<int>
    abstract Decrement : unit -> Async<int>
    abstract Value     : Async<int>

type IResultAggregator<'T> =
    abstract Capacity : int
    abstract IsCompleted : Async<bool>
    abstract SetResult : index:int * value:'T * overwrite:bool -> Async<bool>
    abstract ToArray : unit -> Async<'T []>

[<NoEquality; NoComparison>]
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException

type IResultCell<'T> =
    abstract GetResult : unit -> Async<Result<'T>>
    abstract SetResult : Result<'T> -> Async<unit>

type IRuntimeResourceManager =
    abstract RequestCancellationToken : ?parents:ICloudCancellationToken[] -> Async<ICloudCancellationTokenSource>
    abstract RequestCounter : initialValue:int -> Async<ICloudCounter>
    abstract RequestResultAggregator<'T> : capacity:int -> Async<IResultAggregator<'T>>
    abstract RequestResultCell<'T> : unit -> Async<IResultCell<'T>>

/// A job is a computation belonging to a larger cloud process
/// that is executed by a single worker machine.
[<NoEquality; NoComparison>]
type Job = 
    {
        /// Process info
        ProcessId : string
        /// Vagabond dependencies for computation
        Dependencies : AssemblyId []
        /// Job unique identifier
        JobId : string
        /// Job type
        Type : Type
        /// Triggers job execution with worker-provided execution context
        StartJob : ExecutionContext -> unit
        /// Job fault policy
        FaultPolicy : FaultPolicy
        /// Exception continuation
        Econt : ExecutionContext -> ExceptionDispatchInfo -> unit
        /// Distributed cancellation token source bound to job
        CancellationToken : ICloudCancellationToken
    }
with
    /// <summary>
    ///     Create a cloud job out of given cloud workflow and continuations.
    /// </summary>
    /// <param name="processId">Process id for job.</param>
    /// <param name="dependencies">Dependencies for the job.</param>
    /// <param name="token">Cancellation token for job.</param>
    /// <param name="sc">Success continuation.</param>
    /// <param name="ec">Exception continuation.</param>
    /// <param name="cc">Cancellation continuation.</param>
    /// <param name="wf">Cloud workflow to be run.</param>
    static member Create processId dependencies token faultPolicy sc ec cc (wf : Cloud<'T>) =
        let jobId = mkUUID()
        let runJob ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)

        { 
            ProcessId = processId
            Dependencies = dependencies
            JobId = jobId
            Type = typeof<'T>
            StartJob = runJob
            FaultPolicy = faultPolicy
            Econt = ec
            CancellationToken = token
        }

type IJobLeaseToken =
    abstract FaultCount       : int
    abstract FaultThreshold   : TimeSpan
    abstract GetJob           : unit -> Async<Job>
    abstract Dependencies     : AssemblyId []
    abstract DeclareCompleted : unit -> Async<unit>
    abstract DeclareFaulted   : unit -> Async<unit>

type IJobQueue =
    abstract Enqueue : job:Job * ?target:IWorkerRef -> Async<unit>
    abstract BatchEnqueue : jobs:(Job * IWorkerRef option) [] -> Async<unit>
    abstract Dequeue : id:IWorkerRef -> Async<IJobLeaseToken>