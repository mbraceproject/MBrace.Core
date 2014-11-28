namespace Nessos.MBrace

open Nessos.MBrace.Continuation

// Cloud<'T> is a continuation-based computation that can be distributed.
// It takes two parameters, an ExecutionContext and a continuation triple.
// Importantly, the two values must remain distinct in order for distribution
// to be actuated effectively. ExecutionContext contains resources specific
// to the local executing process (like System.Threading.CancellationToken or open sockets)
// and is therefore not serializable, whereas Continuation<'T> is intended for
// distribution. This is the reason why continuations themselves carry the type signature
//
//      ExecutionContext -> contValue -> unit
//
// to avoid capturing local-only state in closures. In other words, this means that
// cloud workflows form a continuation over reader monad.

/// Representation of a cloud computation, which, when run 
/// will produce a value of type 'T, or raise an exception.
type Cloud<'T> = internal Body of (ExecutionContext -> Continuation<'T> -> unit)

/// Adding this attribute to a let-binding marks that
/// the value definition contains cloud expressions.
type CloudAttribute = ReflectedDefinitionAttribute

/// Disable static check warnings being generated for current workflow.
[<Sealed>]
type NoWarnAttribute() = inherit System.Attribute()

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