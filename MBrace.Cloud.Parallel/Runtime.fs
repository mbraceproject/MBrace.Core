namespace Nessos.MBrace.Runtime

    open System
    open System.Threading
    open System.Threading.Tasks

    open Nessos.MBrace

    type IWorkerRef =
        abstract Id : string

    type RuntimeInfo =
        {
            ProcessId : string
            TaskId : string
            CurrentWorker : IWorkerRef
            Cluster : IWorkerRef []
        }

    type IRuntimeInfoProvider =
        abstract GetRuntimeInfo : unit -> Async<RuntimeInfo>

    type IDistributionProvider =

        abstract Parallel : seq<Cloud<'T>> -> Cloud<'T []>
        abstract Choice : seq<Cloud<'T option>> -> Cloud<'T option>
        abstract SendToWorker : IWorkerRef -> Cloud<'T> -> Cloud<'T> // TODO : implement an ITask<'T>

    /// Storage

    type ICloudRef =
        inherit ICloudDisposable
        abstract GetValueBoxed : unit -> Async<obj>

    type ICloudRef<'T> =
        inherit ICloudRef
        abstract GetValue : unit -> Async<'T>

    type IStorageProvider =
        abstract CreateCloudRef : 'T -> Async<ICloudRef<'T>>
        abstract GetCloudRefs : path:string -> Async<ICloudRef []>

//    //--------------------------------------------------------------------------//
//    //  Collection of resources for describing an abstract distributed runtime  //
//    //--------------------------------------------------------------------------//
//
//    /// Abstract, potentially distributed cancellation token source implementation
//    type ICancellationTokenSource =
//        /// Cancels this cancellation token source
//        abstract Cancel : unit -> unit
//        /// Gets a System.Threading.CancellationToken that subscribes to this source
//        abstract GetCancellationToken : unit -> System.Threading.CancellationToken
//        /// Creates a sub cancellation token source
//        abstract CreateLinkedCancellationTokenSource : unit -> ICancellationTokenSource
//
//    /// Abstract, potentially distributed latch type
//    type ILatch =
//        /// Increment the latch
//        abstract Incr : unit -> int
//        /// Decrement the latch
//        abstract Decr : unit -> int
//        /// Get current latch value
//        abstract Value : int
//
//    /// Abstract latch factory
//    and ILatchFactory =
//        /// Create a new latch with given initial value.
//        abstract Create : initialValue:int -> ILatch
//
//    /// Abstract, potentially disitributed result aggregator
//    type IResultAggregator<'T> =
//
//        /// <summary>
//        ///     Registers a result at given index position  
//        /// </summary>
//        /// <param name="index">index position</param>
//        /// <param name="value">result value.</param>
//        /// <returns>a boolean indicating whether the aggregator is up to capacity.</returns>
//        abstract SetResult : index:int * value:'T -> bool
//        
//        /// Number of aggregated values
//        abstract Count : int
//        /// Current aggregator capacity
//        abstract Capacity : int
//
//        /// Returns the aggregated results in the form of an array
//        abstract ToArray : unit -> 'T []
//
//    /// Abstract result aggregator factory
//    and IResultAggregatorFactory =
//        /// Creates a new result aggregator with given capacity.
//        abstract Create<'T> : capacity : int -> IResultAggregator<'T>
//
//
//    type SchedulingContext =
//        | Sequential
//        | Threadpool
//
//    /// Abstract scheduler implementation
//    type IScheduler =
//        /// Gets or sets the scheduling context
//        abstract Context : SchedulingContext with get,set
//
//        /// Enqueue a cloud workflow for execution with given callbacks and cancellation token source
//        abstract Enqueue : ('T -> unit) -> (exn -> unit) 
//                                        -> (OperationCanceledException -> unit) 
//                                        -> ICancellationTokenSource 
//                                        -> Cloud<'T> -> unit
//
//        /// Schedule a top-level cloud computation as a standalone task
//        abstract ScheduleAsTask : workflow:Cloud<'T> * ?cancellationToken:CancellationToken -> Task<'T>