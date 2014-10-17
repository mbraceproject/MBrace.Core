namespace Nessos.MBrace.Runtime

    open Nessos.MBrace

    /// <summary>
    ///     Executing runtime abstraction.
    /// </summary>
    type IRuntimeProvider =

        /// Get cloud process identifier
        abstract ProcessId : string
        /// Get cloud task identifier
        abstract TaskId : string
        /// Get all available workers in cluster
        abstract GetAvailableWorkers : unit -> Async<IWorkerRef []>
        /// Gets currently running worker
        abstract CurrentWorker : IWorkerRef

        /// <summary>
        ///     Creates a new scheduler instance with updated scheduling context
        /// </summary>
        /// <param name="newContext">new scheduling context</param>
        abstract WithSchedulingContext : newContext:SchedulingContext -> IRuntimeProvider

        /// <summary>
        ///     Gets the current scheduling context.
        /// </summary>
        abstract SchedulingContext : SchedulingContext

        /// <summary>
        ///     Parallel fork/join implementation.
        /// </summary>
        /// <param name="computations">Computations to be executed.</param>
        abstract ScheduleParallel : computations:seq<Cloud<'T>> -> Cloud<'T []>

        /// <summary>
        ///     Parallel nondeterministic choice implementation.
        /// </summary>
        /// <param name="computations">Computations to be executed.</param>
        abstract ScheduleChoice : computations:seq<Cloud<'T option>> -> Cloud<'T option>

        /// <summary>
        ///     Start a new computation as child.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="target">Explicitly specify a target worker for execution.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        abstract ScheduleStartChild : workflow:Cloud<'T> * ?target:IWorkerRef * ?timeoutMilliseconds:int -> Cloud<Cloud<'T>>


    /// <summary>
    ///     Storage abstraction provider.
    /// </summary>
    and IStorageProvider =
        /// <summary>
        ///     Creates a new cloud ref for given value.
        /// </summary>
        /// <param name="value">Cloud ref value.</param>
        abstract CreateCloudRef : value:'T -> Async<ICloudRef<'T>>