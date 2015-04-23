namespace MBrace.Flow

open Nessos.Streams
open MBrace.Core

/// Collects elements into a mutable result container.
type Collector<'T, 'R> = 
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option
    /// Gets an iterator over the elements.
    abstract Iterator : unit -> ParIterator<'T>
    /// The result of the collector.
    abstract Result : 'R

/// Represents a distributed Stream of values.
type CloudFlow<'T> =

    /// Declared degree of parallelism by the workflow.
    abstract DegreeOfParallelism : int option

    /// <summary>
    ///     Creates a cloud workflow that applies CloudFlow to provided evaluation functions.
    /// </summary>
    /// <param name="collectorFactory">Local in-memory collector factory.</param>
    /// <param name="projection">Projection function to intermediate result.</param>
    /// <param name="combiner">Result combiner.</param>
    abstract WithEvaluators<'S, 'R> : collectorFactory : Local<Collector<'T, 'S>> -> 
                                        projection : ('S -> Local<'R>) -> 
                                        combiner : ('R []  -> Local<'R>) -> Cloud<'R>