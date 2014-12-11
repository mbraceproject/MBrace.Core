namespace Nessos.MBrace

open System
open System.Runtime.Serialization

/// Scheduling context for currently executing cloud process.
type SchedulingContext =
    /// Current thread scheduling context
    | Sequential
    /// Thread pool scheduling context
    | ThreadParallel
    /// Distributed scheduling context
    | Distributed

/// Denotes a reference to a worker node in the cluster
type IWorkerRef =
    /// Worker type identifier
    abstract Type : string
    /// Worker unique identifier
    abstract Id : string

/// Exception indicating fault in MBrace runtime
[<AutoSerializable(true)>]
type FaultException =
    inherit Exception

    new() = { inherit Exception() }
    new(message : string) = { inherit Exception(message)}
    new(message : string, innerException : exn) = { inherit Exception(message, innerException) }
    new (sI : SerializationInfo, sC : StreamingContext) = { inherit Exception(sI, sC) }

/// Fault recovery policy used in runtime execution;
/// takes number of attempts and fault exception returning
/// the recovery action: either fail or retry after given delay.
type FaultPolicy = { Policy : int -> exn -> TimeSpan option }
with
    /// Makes no attempt at retrying, raising fault exception at the first occurrence.
    static member NoRetry = { Policy = fun _ _ -> None }

    /// <summary>
    ///     Forever re-attempt faulted computations.
    /// </summary>
    /// <param name="delay">Delay before each retry. Defaults to zero.</param>
    static member InfiniteRetry (?delay : TimeSpan) = 
        let delay = defaultArg delay TimeSpan.Zero
        { Policy = fun _ _ -> Some  delay }

    /// <summary>
    ///     Retries at most a given number of times.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="delay">Delay before each retry. Defaults to zero.</param>
    static member Retry(maxRetries : int, ?delay : TimeSpan) =
        if maxRetries < 0 then invalidArg "maxRetries" "must be non-negative."
        let delay = defaultArg delay TimeSpan.Zero
        { Policy = fun retries _ ->
            if retries > maxRetries then None
            else Some delay }
        
    /// <summary>
    ///     Retries as long as exception of given type is raised.
    /// </summary>
    /// <param name="delay">Delay before each retry. Defaults to zero.</param>
    static member Filter<'exn when 'exn :> Exception>(?delay : TimeSpan) =
        let delay = defaultArg delay TimeSpan.Zero
        { Policy = fun _ e -> 
            match e with
            | :? 'exn -> Some delay
            | _ -> None }

    /// <summary>
    ///     Exponentially delays after each retry.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial delay. Defaults to 50ms</param>
    static member ExponentialDelay(maxRetries : int, ?initialDelay : TimeSpan) =
        let initialDelay = defaultArg initialDelay <| TimeSpan.FromMilliseconds 50.
        { Policy = fun retries _ ->
            if retries > maxRetries then None
            else
                let ms = initialDelay.TotalMilliseconds * (2. ** float (retries - 1))
                Some <| TimeSpan.FromMilliseconds ms
        }

    /// <summary>
    ///     Retries with delay mapped to attempt number.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="delayF">Delay mapping function.</param>
    static member MapDelay(maxRetries : int, delayF : int -> TimeSpan) =
        { Policy = fun retries _ ->
            if retries > maxRetries then None
            else
                Some (delayF retries)
        }