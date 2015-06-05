namespace MBrace.Runtime

open System

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store

open MBrace.Runtime.Utils

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

/// Defines a distributed, atomic counter.
type ICloudCounter =
    inherit IAsyncDisposable
    /// Asynchronously increments the counter by 1.
    abstract Increment : unit -> Async<int>
    /// Asynchronously decrements the counter by 1.
    abstract Decrement : unit -> Async<int>
    /// Asynchronously fetches the current value for the counter.
    abstract Value     : Async<int>

/// Defines a distributed result aggregator.
type IResultAggregator<'T> =
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
    /// <param name="overwrite">Overwrite if value already exists at index.</param>
    abstract SetResult : index:int * value:'T * overwrite:bool -> Async<bool>
    /// <summary>
    ///     Asynchronously returns aggregated results from aggregator.
    /// </summary>
    abstract ToArray : unit -> Async<'T []>


/// Defines a distributed task completion source.
type ICloudTaskCompletionSource =
    inherit IAsyncDisposable
    /// <summary>
    ///     Asynchronously sets the the result to be an exception.
    /// </summary>
    /// <param name="edi">Exception dispatch info to be submitted.</param>
    abstract SetException : edi:ExceptionDispatchInfo -> Async<unit>

    /// <summary>
    ///     Asynchronously declares the the result to be cancelled.
    /// </summary>
    /// <param name="edi">Exception dispatch info to be submitted.</param>
    abstract SetCancelled : exn:OperationCanceledException -> Async<unit>

/// Defines a distributed task completion source.
type ICloudTaskCompletionSource<'T> =
    inherit ICloudTaskCompletionSource
    /// Gets the underlying cloud task for the task completion source.
    abstract Task : ICloudTask<'T>
    /// Asynchronously sets a completed result for the task.
    abstract SetCompleted : 'T -> Async<unit>