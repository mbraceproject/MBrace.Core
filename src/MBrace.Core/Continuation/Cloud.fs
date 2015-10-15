namespace MBrace.Core

open System.Runtime.Serialization

open MBrace.Core.Internals

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
type internal Body<'T> = ExecutionContext -> Continuation<'T> -> unit

/// Represents a distributed cloud computation. Constituent parts of the computation
/// may be serialized, scheduled and may migrate between different distributed 
/// execution contexts. If the computation accesses concurrent shared memory then
/// replace by a more constrained "local" workflow.  
/// When run will produce a value of type 'T, or raise an exception. 
[<DataContract>]
type Cloud<'T> =
    [<DataMember(Name = "Body")>]
    val mutable private body : Body<'T>
    internal new (body : Body<'T>) = { body = body }
    member internal __.Body = __.body

/// Represents a machine-constrained cloud computation. The
/// computation runs to completion as a locally executing in-memory 
/// computation. The computation may access concurrent shared memory and 
/// unserializable resources.  When run will produce 
/// a value of type 'T, or raise an exception.
[<Sealed; DataContract>]
type CloudLocal<'T> internal (body : Body<'T>) = 
    inherit Cloud<'T>(body)