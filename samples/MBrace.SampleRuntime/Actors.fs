namespace Nessos.MBrace.SampleRuntime.Actors

//
//  Implements a collection of distributed resources that provide
//  coordination for execution in the distributed runtime.
//  The particular implementations are done using Thespian,
//  a distributed actor framework for F#.
//

open System
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

open Nessos.Vagrant

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Vagrant
open Nessos.MBrace.SampleRuntime

/// Actor publication utilities
type Actor private () =
    static do Config.initRuntimeState()

    /// Publishes an actor instance to the default TCP protocol
    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.utcp() ]
        |> Actor.start

    /// Exception-safe stateful actor behavior combinator
    static member Stateful (init : 'State) f = 
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async { 
                try return! f state msg 
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// Exception-safe stateless actor behavior combinator
    static member Stateless (f : 'T -> Async<unit>) =
        Actor.Stateful () (fun () t -> f t)

//
//  Distributed latch implementation
//

type private LatchMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed latch implementation
type Latch private (source : ActorRef<LatchMessage>) =
    /// Atomically increment the latch
    member __.Increment () = source <!- Increment
    /// Returns the current latch value
    member __.Value = source <!= GetValue
    /// Initialize a new latch instance in the current process
    static member Init(init : int) =
        let behaviour count msg = async {
            match msg with
            | Increment rc ->
                do! rc.Reply (count + 1)
                return (count + 1)
            | GetValue rc ->
                do! rc.Reply count
                return count
        }

        let ref =
            Actor.Stateful init behaviour
            |> Actor.Publish
            |> Actor.ref

        new Latch(ref)

//
//  Distributed readable cell
//

type Cell<'T> private (source : ActorRef<IReplyChannel<'T>>) =
    member __.GetValue () = source <!- id
    /// Initialize a distributed cell from a value factory ; assume exception safe
    static member Init (f : unit -> 'T) =
        let ref =
            Actor.Stateless (fun (rc : IReplyChannel<'T>) -> rc.Reply (f ()))
            |> Actor.Publish
            |> Actor.ref

        new Cell<'T>(ref)

//
//  Distributed logger
//

type Logger private (target : ActorRef<string>) =
    interface ICloudLogger with member __.Log txt = target <-- txt
    static member Init(logger : string -> unit) =
        let ref =
            Actor.Stateless (fun msg -> async { return logger msg })
            |> Actor.Publish
            |> Actor.ref

        new Logger(ref)

//
//  Distributed result aggregator
//

type private ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * completed:IReplyChannel<bool>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

/// A distributed resource that aggregates an array of results.
type ResultAggregator<'T> private (source : ActorRef<ResultAggregatorMsg<'T>>) =
    /// Asynchronously assign a value at given index.
    member __.SetResult(index : int, value : 'T) = source <!- fun ch -> SetResult(index, value, ch)
    /// Results the completed
    member __.ToArray () = source <!- ToArray
    /// Initializes a result aggregator of given size at the current process.
    static member Init(size : int) =
        let behaviour (results : Map<int, 'T>) msg = async {
            match msg with
            | SetResult(i, value, rc) when i < 0 || i >= size ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = size
                do! rc.Reply isCompleted
                return results

            | IsCompleted rc ->
                do! rc.Reply ((results.Count = size))
                return results

            | ToArray rc when results.Count = size ->
                let array = results |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray
                do! rc.Reply array
                return results

            | ToArray rc ->
                let e = new InvalidOperationException("Result aggregator incomplete.")
                do! rc.ReplyWithException e
                return results
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new ResultAggregator<'T>(ref)

//
//  Distributed result cell
//

/// Result value
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled edi -> ExceptionDispatchInfo.raiseWithCurrentStackTrace true edi

type private ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

/// Defines a reference to a distributed result cell instance.
type ResultCell<'T> private (source : ActorRef<ResultCellMsg<'T>>) =
    /// Try setting the result
    member c.SetResult result = source <!- fun ch -> SetResult(result, ch)
    /// Try getting the result
    member c.TryGetResult () = source <!- TryGetResult
    /// Asynchronously poll for result
    member c.AwaitResult() = async {
        let! result = source <!- TryGetResult
        match result with
        | None -> 
            do! Async.Sleep 500
            return! c.AwaitResult()
        | Some r -> return r
    }

    /// Initialize a new result cell in the local process
    static member Init() : ResultCell<'T> =
        let behavior state msg = async {
            match msg with
            | SetResult (_, rc) when Option.isSome state -> 
                do! rc.Reply false
                return state
            | SetResult (result, rc) ->
                do! rc.Reply true
                return (Some result)

            | TryGetResult rc ->
                do! rc.Reply state
                return state
        }

        let ref =
            Actor.Stateful None behavior
            |> Actor.Publish
            |> Actor.ref

        new ResultCell<'T>(ref)

//
//  Distributed Cancellation token sources
//

type private CancellationTokenId = string

type private CancellationTokenMsg =
    | IsCancellationRequested of IReplyChannel<bool>
    | RegisterChild of DistributedCancellationTokenSource
    | Cancel

/// Defines a distributed cancellation token source that can be cancelled
/// in the context of a distributed runtime.
and DistributedCancellationTokenSource private (source : ActorRef<CancellationTokenMsg>) =
    member __.Cancel () = source <-- Cancel
    member __.IsCancellationRequested () = source <!- IsCancellationRequested
    member private __.RegisterChild ch = source <-- RegisterChild ch
    /// Creates a System.Threading.CancellationToken that is linked
    /// to the distributed cancellation token.
    member __.GetLocalCancellationToken() =
        let cts = new System.Threading.CancellationTokenSource()

        let rec checkCancellation () = async {
            let! isCancelled = Async.Catch(source <!- IsCancellationRequested)
            match isCancelled with
            | Choice1Of2 true -> cts.Cancel()
            | Choice1Of2 false ->
                do! Async.Sleep 500
                return! checkCancellation ()
            | Choice2Of2 e ->
                do! Async.Sleep 1000
                return! checkCancellation ()
        }

        do Async.Start(checkCancellation())
        cts.Token

    /// <summary>
    ///     Initializes a new distributed cancellation token source in the current process
    /// </summary>
    /// <param name="parent">Linked parent cancellation token source</param>
    static member Init(?parent : DistributedCancellationTokenSource) =
        let behavior ((isCancelled, children) as state) msg = async {
            match msg with
            | IsCancellationRequested rc ->
                do! rc.Reply isCancelled
                return state
            | RegisterChild child when isCancelled ->
                try child.Cancel() with _ -> ()
                return state
            | RegisterChild child ->
                return (isCancelled, child :: children)
            | Cancel ->
                for ch in children do try ch.Cancel() with _ -> ()
                return (true, [])
        }

        let ref =
            Actor.Stateful (false, []) behavior
            |> Actor.Publish
            |> Actor.ref

        let dcts = new DistributedCancellationTokenSource(ref)

        match parent with
        | None -> ()
        | Some p -> p.RegisterChild dcts

        dcts

//
//  Distributed lease monitor. Tracks progress of dequeued tasks by 
//  requiring heartbeats from the worker node. Triggers a fault event
//  when heartbeat threshold is exceeded. Used for the sample fault-tolerance implementation.
//

type private LeaseState =
    | Acquired
    | Released
    | Faulted

type private LeaseMonitorMsg =
    | SetLeaseState of LeaseState
    | GetLeaseState of IReplyChannel<LeaseState>

/// Distributed lease monitor instance
type LeaseMonitor private (threshold : TimeSpan, source : ActorRef<LeaseMonitorMsg>) =
    /// Declare lease to be released successfuly
    member __.Release () = source <-- SetLeaseState Released
    /// Declare fault during lease
    member __.DeclareFault () = source <-- SetLeaseState Faulted
    /// Heartbeat fault threshold
    member __.Threshold = threshold
    /// Initializes an asynchronous hearbeat sender workflow
    member __.InitHeartBeat () = async {
        let! ct = Async.CancellationToken
        let cts = CancellationTokenSource.CreateLinkedTokenSource ct
        let rec heartbeat () = async {
            try source <-- SetLeaseState Acquired with _ -> ()
            do! Async.Sleep (int threshold.TotalMilliseconds / 2)
            return! heartbeat ()
        }

        Async.Start(heartbeat(), cts.Token)
        return { new IDisposable with member __.Dispose () = cts.Cancel () }
    }
    
    /// <summary>
    ///     Initializes a new lease monitor.
    /// </summary>
    /// <param name="threshold">Heartbeat fault threshold.</param>
    static member Init (threshold : TimeSpan) =
        let behavior ((ls, lastRenew : DateTime) as state) msg = async {
            match msg, ls with
            | SetLeaseState _, (Faulted | Released) -> return state
            | SetLeaseState ls', Acquired -> return (ls', DateTime.Now)
            | GetLeaseState rc, Acquired when DateTime.Now - lastRenew > threshold ->
                do! rc.Reply Faulted
                return (Faulted, lastRenew)
            | GetLeaseState rc, ls ->
                do! rc.Reply ls
                return state
        }

        let actor =
            Actor.Stateful (Acquired, DateTime.Now) behavior
            |> Actor.Publish

        let faultEvent = new Event<unit> ()
        let rec poll () = async {
            let! state = actor.Ref <!- GetLeaseState
            match state with
            | Acquired -> 
                do! Async.Sleep(2 * int threshold.TotalMilliseconds)
                return! poll ()
            | Released -> try actor.Stop() with _ -> ()
            | Faulted -> try faultEvent.Trigger () ; actor.Stop() with _ -> () 
        }

        Async.Start(poll ())

        faultEvent.Publish, new LeaseMonitor(threshold, actor.Ref)

//
//  Distributed, fault-tolerant queue implementation
//

type private QueueMsg<'T> =
    | EnQueue of 'T
    | TryDequeue of IReplyChannel<('T * LeaseMonitor) option>

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
type Queue<'T> private (source : ActorRef<QueueMsg<'T>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue t
    member __.TryDequeue () = source <!- TryDequeue

    /// Initializes a new distribued queue instance.
    static member Init() =
        let self = ref Unchecked.defaultof<ActorRef<QueueMsg<'T>>>
        let behaviour (queue : ImmutableQueue<'T>) msg = async {
            match msg with
            | EnQueue t -> return queue.Enqueue t
            | TryDequeue rc ->
                match queue.TryDequeue() with
                | None ->
                    do! rc.Reply None 
                    return queue

                | Some(t, queue') ->
                    let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                    do! rc.Reply (Some (t, leaseMonitor))
                    let _ = putBack.Subscribe(fun () -> self.Value <-- EnQueue t)
                    return queue'
        }

        self :=
            Actor.Stateful ImmutableQueue<'T>.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new Queue<'T>(self.Value)

//
//  Defines a distributed channel implementation
//

type private ChannelMsg<'T> =
    | Send of 'T
    | Receive of IReplyChannel<'T>

type Channel<'T> private (source : ActorRef<ChannelMsg<'T>>) =

    interface IReceivePort<'T> with
        member __.Receive(?timeout : int) = source.PostWithReply(Receive, ?timeout = timeout)
        member __.Dispose () = async.Zero()

    interface ISendPort<'T> with
        member __.Send(msg : 'T) = source.AsyncPost(Send msg)

    /// Initializes a new distribued queue instance.
    static member Init() =
        let self = ref Unchecked.defaultof<ActorRef<ChannelMsg<'T>>>
        let behaviour (messages : ImmutableQueue<'T>, receivers : ImmutableQueue<IReplyChannel<'T>>) msg = async {
            match msg with
            | Send t ->
                match receivers.TryDequeue () with
                | Some(rc, receivers') ->
                    // receiving side may have timed out a long time ago, protect
                    try 
                        do! rc.Reply t
                        return (messages, receivers')
                    with e ->
                        // reply failed, re-enqueue
                        self.Value <-- Send t
                        return (messages, receivers')

                | None ->
                    return (messages.Enqueue t, receivers)

            | Receive rc ->
                match messages.TryDequeue () with
                | Some(t, senders') ->
                    do! rc.Reply t
                    return (senders', receivers)
                | None ->
                    return messages, receivers.Enqueue rc
        }

        self :=
            Actor.Stateful (ImmutableQueue.Empty, ImmutableQueue.Empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Channel<'T>(self.Value)

//
//  Distributed Resource factory.
//  Provides facility for remotely deploying distributed resources.
//

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

/// Provides facility for remotely deploying resources
type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =
    member __.RequestResource<'T>(factory : unit -> 'T) = async {
        let ctor () = factory () :> obj
        let! resource = source <!- fun ch -> RequestResource(ctor, ch)
        return resource :?> 'T
    }

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestCancellationTokenSource(?parent) = __.RequestResource(fun () -> DistributedCancellationTokenSource.Init(?parent = parent))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())
    member __.RequestChannel<'T>() = __.RequestResource(fun () -> Channel<'T>.Init())

    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = try ctor () |> Choice1Of2 with e -> Choice2Of2 e
            match r with
            | Choice1Of2 res -> do! rc.Reply res
            | Choice2Of2 e -> do! rc.ReplyWithException e
        }

        let ref =
            Actor.Stateless behavior
            |> Actor.Publish
            |> Actor.ref

        new ResourceFactory(ref)

//
// Assembly exporter : provides assembly uploading facility for Vagrant
//

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<AssemblyPackage list> 

/// Provides assembly uploading facility for Vagrant.
type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let packages = VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            do! ch.Reply packages
        }

        let ref = 
            Actor.Stateless behaviour
            |> Actor.Publish
            |> Actor.ref

        new AssemblyExporter(ref)

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
                    member __.PullAssemblies ids = exporter <!- fun ch -> RequestAssemblies(ids, ch)
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


type WorkerManager =
    | SubscribeToRuntime of IReplyChannel<unit> * string * int
    | Unsubscribe of IReplyChannel<unit>
