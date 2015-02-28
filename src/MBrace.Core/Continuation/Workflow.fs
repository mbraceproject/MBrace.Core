namespace MBrace

open MBrace.Continuation

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

/// Scheduling Context annotation
[<AbstractClass>] type SchedulingContext () = class end

[<RequireQualifiedAccess>]
module Context =
    /// Annotates an MBrace workflow with cloud semantics
    [<AbstractClass>] type Cloud private () = inherit SchedulingContext()
    /// Annotates an MBrace workflow with in-memory semantics
    [<AbstractClass>] type Local private () = inherit SchedulingContext()

/// Representation of an MBrace workflow, which, when run 
/// will produce a value of type 'T, or raise an exception.
[<AbstractClass>]
type Workflow<'T> internal (body : ExecutionContext -> Continuation<'T> -> unit) =
    member internal __.Body = body

/// Representation of an MBrace workflow, which, when run 
/// will produce a value of type 'T, or raise an exception.
[<Sealed; AutoSerializable(true)>]
type Workflow<'Ctx, 'T when 'Ctx :> SchedulingContext> internal (body : ExecutionContext -> Continuation<'T> -> unit) =
    inherit Workflow<'T>(body)

/// Representation of an in-memory computation, which, when run 
/// will produce a value of type 'T, or raise an exception.
type Local<'T> = Workflow<Context.Local, 'T>

/// Representation of a cloud computation, which, when run 
/// will produce a value of type 'T, or raise an exception.
type Cloud<'T> = Workflow<Context.Cloud, 'T>

/// Adding this attribute to a let-binding marks that
/// the value definition contains cloud expressions.
type CloudAttribute = ReflectedDefinitionAttribute

/// Disable static check warnings being generated for current workflow.
[<Sealed>]
type NoWarnAttribute() = inherit System.Attribute()

/// Denotes handle to a distributable resource that can be disposed of.
type ICloudDisposable =
    /// Releases any storage resources used by this object.
    abstract Dispose : unit -> Local<unit>