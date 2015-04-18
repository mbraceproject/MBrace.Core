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
    /// The number of concurrently executing tasks
    abstract DegreeOfParallelism : int option
    /// Applies the given collector to the CloudFlow.
    abstract Apply<'S, 'R> : Local<Collector<'T, 'S>> -> ('S -> Local<'R>) -> ('R []  -> Local<'R>) -> Cloud<'R>