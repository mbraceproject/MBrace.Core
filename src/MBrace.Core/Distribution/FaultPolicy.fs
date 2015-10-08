namespace MBrace.Core

open System
open System.Runtime.Serialization

/// Exception indicating fault in MBrace runtime
[<AutoSerializable(true)>]
type FaultException =
    inherit Exception

    new() = { inherit Exception() }
    new(message : string) = { inherit Exception(message)}
    new(message : string, innerException : exn) = { inherit Exception(message, innerException) }
    new (sI : SerializationInfo, sC : StreamingContext) = { inherit Exception(sI, sC) }

/// Specifies the recovery action that should happen
/// in the event of an MBrace fault.
[<NoEquality; NoComparison>]
type FaultRecoveryAction =
    /// Give up on the computation raising a fault exception.
    | ThrowException of exn
    /// Retry the computation after specified delay.
    | Retry of delay:TimeSpan

/// Contains fault data for a given MBrace computation.
[<NoEquality; NoComparison>]
type FaultData =
    {
        /// Number of times the current computation has faulted.
        NumberOfFaults : int
        /// Latest fault exception that was raised.
        FaultException : exn
    }

/// Fault policy to be applied in a distributed MBrace computation.
[<AutoSerializable(true); AbstractClass; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type FaultPolicy () =

    /// FaultPolicy identifier
    abstract Id : string

    /// <summary>
    ///     Decides on the recovery action that should take place using supplied fault data.
    /// </summary>
    /// <param name="numberOfFaults">Number of time that the given computation has faulted.</param>
    /// <param name="faultException">Latest fault exception that was raised.</param>
    abstract GetFaultRecoveryAction : numberOfFaults:int * faultException:exn -> FaultRecoveryAction

    member private __.StructuredFormatDisplay = __.Id
    override __.ToString() = __.Id

    //
    // #region Collection of common fault policy implementations.
    //

    /// Makes no attempt at retrying a faulted computation, raising any exception at the first occurrence.
    static member NoRetry =
        { new FaultPolicy() with 
            member __.Id = "NoRetry"
            member __.GetFaultRecoveryAction(_,e) = ThrowException e }

    /// <summary>
    ///     Forever re-attempt faulted computations.
    /// </summary>
    /// <param name="delay">Delay before each retry. Defaults to zero.</param>
    static member InfiniteRetries (?delay : TimeSpan) = 
        let delay = defaultArg delay TimeSpan.Zero
        { new FaultPolicy() with 
            member __.Id = sprintf "InfiniteRetries (delay: %O)" delay
            member __.GetFaultRecoveryAction(_,_) = Retry delay }

    /// <summary>
    ///     Retries at most a given number of times.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="delay">Delay before each retry. Defaults to zero.</param>
    static member WithMaxRetries(maxRetries : int, ?delay : TimeSpan) =
        if maxRetries < 0 then invalidArg "maxRetries" "must be non-negative."
        let delay = defaultArg delay TimeSpan.Zero
        { new FaultPolicy() with 
            member __.Id = sprintf "WithMaxRetries (maxRetries: %d, delay: %O)" maxRetries delay
            member __.GetFaultRecoveryAction(retries,e) =
                if retries > maxRetries then ThrowException e
                else Retry delay }

    /// <summary>
    ///     Retries computation until a given number, 
    ///     using exponentially increasing delay intervals after each fault.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial delay. Defaults to 50ms</param>
    static member WithExponentialDelay(maxRetries : int, ?initialDelay : TimeSpan) =
        let initialDelay = defaultArg initialDelay <| TimeSpan.FromMilliseconds 50.
        { new FaultPolicy() with
            member __.Id = sprintf "WithExponentialDelay (maxRetries: %d, initialDelay: %O)" maxRetries initialDelay
            member __.GetFaultRecoveryAction(retries,e) =
                if retries > maxRetries then ThrowException e
                else
                    let ms = initialDelay.TotalMilliseconds * (2. ** float (retries - 1))
                    Retry <| TimeSpan.FromMilliseconds ms
        }

    /// <summary>
    ///     Retries with delay mapped to attempt number.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="delayF">Delay mapping function.</param>
    static member WithMappedDelay(maxRetries : int, delayF : int -> TimeSpan) =
        { new FaultPolicy() with
            member __.Id = sprintf "WithMappedDelay (maxRetries: %d)" maxRetries
            member __.GetFaultRecoveryAction(retries,e) =
                if retries > maxRetries then ThrowException e
                else
                    Retry (delayF retries)
        }