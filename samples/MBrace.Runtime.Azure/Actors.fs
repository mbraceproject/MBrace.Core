namespace Nessos.MBrace.Runtime.Azure.Actors

//
//  Implements a collection of distributed resources that provide
//  coordination for execution in the distributed runtime.
//  The particular implementations are done using Thespian,
//  a distributed actor framework for F#.
//

open System
open System.Threading


open Nessos.Vagrant

open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Azure


//
//  Distributed latch implementation
//

/// Distributed latch implementation
type Latch private () =
    /// Atomically increment the latch
    member __.Increment () = failwith "Not implemented"
    /// Returns the current latch value
    member __.Value = failwith "Not implemented"
    /// Initialize a new latch instance in the current process
    static member Init(init : int) = 
        new Latch()

//
//  Distributed readable cell
//

type Cell<'T> private () =
    member __.GetValue () = failwith "Not implemented"
    /// Initialize a distributed cell from a value factory ; assume exception safe
    static member Init (f : unit -> 'T) =
        new Cell<'T>()

//
//  Distributed logger
//

type Logger private () =
    interface ICloudLogger with member __.Log txt = failwith "Not implemented"
    static member Init(logger : string -> unit) =
        new Logger()

//
//  Distributed result aggregator
//

/// A distributed resource that aggregates an array of results.
type ResultAggregator<'T> private () =
    /// Asynchronously assign a value at given index.
    member __.SetResult(index : int, value : 'T) = failwith "Not implemented"
    /// Results the completed
    member __.ToArray () = failwith "Not implemented"
    /// Initializes a result aggregator of given size at the current process.
    static member Init(size : int) =
        new ResultAggregator<'T>()

//
//  Distributed result cell
//

/// Result value
type Result<'T> =
    | Completed of 'T
    | Exception of exn
    | Cancelled of exn
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception e -> raiseWithCurrentStacktrace e
        | Cancelled e -> raiseWithCurrentStacktrace e 

/// Defines a reference to a distributed result cell instance.
type ResultCell<'T> private () =
    /// Try setting the result
    member c.SetResult result = failwith "Not implemented"
    /// Try getting the result
    member c.TryGetResult () = failwith "Not implemented"
    /// Asynchronously poll for result
    member c.AwaitResult() = failwith<Async<'T ref>> "Not implemented"

    /// Initialize a new result cell in the local process
    static member Init() : ResultCell<'T> =
        new ResultCell<'T>()

//
//  Distributed Cancellation token sources
//

/// Defines a distributed cancellation token source that can be cancelled
/// in the context of a distributed runtime.
and DistributedCancellationTokenSource private () =
    member __.Cancel () = failwith "Not implemented"
    member __.IsCancellationRequested () = failwith "Not implemented"
    member private __.RegisterChild ch = failwith "Not implemented"
    /// Creates a System.Threading.CancellationToken that is linked
    /// to the distributed cancellation token.
    member __.GetLocalCancellationToken() = failwith "Not implemented"

    /// <summary>
    ///     Initializes a new distributed cancellation token source in the current process
    /// </summary>
    /// <param name="parent">Linked parent cancellation token source</param>
    static member Init(?parent : DistributedCancellationTokenSource) =      
        let dcts = new DistributedCancellationTokenSource()
        dcts

//
//  Distributed lease monitor. Tracks progress of dequeued tasks by 
//  requiring heartbeats from the worker node. Triggers a fault event
//  when heartbeat threshold is exceeded. Used for the sample fault-tolerance implementation.
//

/// Distributed lease monitor instance
type LeaseMonitor private () =
    /// Declare lease to be released successfuly
    member __.Release () = failwith "Not implemented"
    /// Declare fault during lease
    member __.DeclareFault () = failwith "Not implemented"
    /// Heartbeat fault threshold
    member __.Threshold = failwith "Not implemented"
    /// Initializes an asynchronous hearbeat sender workflow
    member __.InitHeartBeat () = failwith "Not implemented"

    
    /// <summary>
    ///     Initializes a new lease monitor.
    /// </summary>
    /// <param name="threshold">Heartbeat fault threshold.</param>
    static member Init (threshold : TimeSpan) =
       failwith "Not implemented"

//
//  Distributed, fault-tolerant queue implementation
//

type private ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
    static member Empty = new ImmutableQueue<'T>([],[])
    member __.Enqueue t = new ImmutableQueue<'T>(front, t :: back)
    member __.TryDequeue () = 
        match front with
        | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, back))
        | [] -> 
            match List.rev back with
            | [] -> None
            | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, []))

/// Provides a distributed, fault-tolerant queue implementation
type Queue<'T> private () =
    member __.Enqueue (t : 'T) = failwith "Not implemented"
    member __.TryDequeue () = failwith "Not implemented"

    /// Initializes a new distributed queue instance.
    static member Init() =
        new Queue<'T>()


/// Provides facility for remotely deploying resources
type ResourceFactory private () =
    member __.RequestResource<'T>(factory : unit -> 'T) = failwith<Async<'T>> "Not implemented"

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestCancellationTokenSource(?parent) = __.RequestResource(fun () -> DistributedCancellationTokenSource.Init(?parent = parent))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())

    static member Init () =
        new ResourceFactory()

//
// Assembly exporter : provides assembly uploading facility for Vagrant
//


/// Provides assembly uploading facility for Vagrant.
type AssemblyExporter private () =
    static member Init() =
        new AssemblyExporter()

    /// <summary>
    ///     Request the loading of assembly dependencies from remote
    ///     assembly exporter to the local application domain.
    /// </summary>
    /// <param name="ids">Assembly id's to be loaded in app domain.</param>
    member __.LoadDependencies(ids : AssemblyId list) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return ids }
                    member __.PullAssemblies ids = failwith "Not implemented"
            }

        do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
    }

    /// <summary>
    ///     Compute assembly dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be analyzed</param>
    member __.ComputeDependencies (graph:'T) =
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId
