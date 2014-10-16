namespace Nessos.MBrace.Runtime

    open Nessos.MBrace

    /// <summary>
    ///     Distributed computation scheduling abstraction
    /// </summary>
    type ISchedulingProvider =

        /// <summary>
        ///     Creates a new scheduler instance with updated scheduling context
        /// </summary>
        /// <param name="newContext">new scheduling context</param>
        abstract WithContext : newContext:SchedulingContext -> ISchedulingProvider

        /// <summary>
        ///     Gets the current scheduling context.
        /// </summary>
        abstract Context : SchedulingContext

        /// <summary>
        ///     Parallel fork/join implementation.
        /// </summary>
        /// <param name="computations">Computations to be executed.</param>
        abstract Parallel : computations:seq<Cloud<'T>> -> Cloud<'T []>

        /// <summary>
        ///     Parallel nondeterministic choice implementation.
        /// </summary>
        /// <param name="computations">Computations to be executed.</param>
        abstract Choice : computations:seq<Cloud<'T option>> -> Cloud<'T option>

        /// <summary>
        ///     Start a new computation as child.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="target">Explicitly specify a target worker for execution.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        abstract StartChild : workflow:Cloud<'T> * ?target:IWorkerRef * ?timeoutMilliseconds:int -> Cloud<Cloud<'T>>

        /// <summary>
        ///     Get current runtime information.
        /// </summary>
        abstract GetRuntimeInfo : unit -> Async<RuntimeInfo>


    /// <summary>
    ///     Storage abstraction provider
    /// </summary>
    type IStorageProvider =
        /// <summary>
        ///     Creates a new cloud ref for given value.
        /// </summary>
        /// <param name="value">Cloud ref value.</param>
        abstract CreateCloudRef : value:'T -> Async<ICloudRef<'T>>