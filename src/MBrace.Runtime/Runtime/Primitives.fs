namespace MBrace.Runtime

open MBrace.Core.Internals

/// Defines a serializable cancellation entry with global visibility that can
/// be cancelled or polled for cancellation.
type ICancellationEntry =
    inherit IAsyncDisposable
    /// Unique identifier for cancellation entry instance.
    abstract UUID : string
    /// Asynchronously checks if entry has been cancelled.
    abstract IsCancellationRequested : Async<bool>
    /// Asynchronously cancels the entry.
    abstract Cancel : unit -> Async<unit>

/// Defines a serializable cancellation entry manager with global visibility.
type ICancellationEntryFactory =
    /// Asynchronously creates a cancellation entry with no parents.
    abstract CreateCancellationEntry : unit -> Async<ICancellationEntry>

    /// <summary>
    ///     Asynchronously creates a cancellation entry provided parents. 
    ///     Returns 'None' if all parents have been cancelled.
    /// </summary>
    /// <param name="parents">Cancellation entry parents.</param>
    abstract TryCreateLinkedCancellationEntry : parents:ICancellationEntry[] -> Async<ICancellationEntry option>

/// Defines a distributed counter entity.
type ICloudCounter =
    inherit IAsyncDisposable
    /// Asynchronously increments counter by 1, returning the updated count
    abstract Increment : unit -> Async<int64>
    /// Asynchronously fetches the current value for the counter.
    abstract Value       : Async<int64>

/// Defines a cloud counter factory abstraction.
type ICloudCounterFactory =

    /// <summary>
    ///     Creates a new counter with provided initial value.
    /// </summary>
    /// <param name="initialValue">Initial value for counter.</param>
    abstract CreateCounter : initialValue:int64 -> Async<ICloudCounter>

/// Defines a serializable, distributed result aggregator entity.
type ICloudResultAggregator<'T> =
    inherit IAsyncDisposable
    /// Declared capacity for result aggregator.
    abstract Capacity : int
    /// Asynchronously returns current accumulated size for aggregator.
    abstract CurrentSize : Async<int>
    /// Asynchronously returns if result aggregator has been completed.
    abstract IsCompleted : Async<bool>

    /// <summary>
    ///     Asynchronously sets result at given index to aggregator.
    ///     Returns true if aggregation is completed.
    /// </summary>
    /// <param name="index">Index to set value at.</param>
    /// <param name="value">Value to be set.</param>
    /// <param name="workerId">Worker identifier of setter process.</param>
    abstract SetResult : index:int * value:'T * workerId:IWorkerId -> Async<bool>

    /// Asynchronously returns aggregated results, provided aggregation is completed.
    abstract ToArray : unit -> Async<'T []>

/// Defines a factory for creating runtime primitives.
type ICloudResultAggregatorFactory =

    /// <summary>
    ///     Allocates a new cloud result aggregator factory.
    /// </summary>
    /// <param name="aggregatorId">Unique result aggregator identifier.</param>
    /// <param name="capacity">Declared capacity of result aggregator.</param>
    abstract CreateResultAggregator<'T> : aggregatorId:string * capacity:int -> Async<ICloudResultAggregator<'T>>