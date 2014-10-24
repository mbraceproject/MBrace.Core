namespace Nessos.MBrace

open Nessos.MBrace.Runtime

/// Representation of a cloud computation, which, when run 
/// will produce a value of type 'T, or raise an exception.
type Cloud<'T> = internal Body of (ExecutionContext -> Continuation<'T> -> unit)

/// Denotes handle to a distributable resource that can be disposed of.
type ICloudDisposable =
    /// Releases any storage resources used by this object.
    abstract Dispose : unit -> Async<unit>

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