namespace MBrace.SampleRuntime.Actors

//
//  Implements a collection of distributed resources that provide
//  coordination for execution in the distributed runtime.
//  The particular implementations are done using Thespian,
//  a distributed actor framework for F#.
//

open System
open System.Threading
open System.Runtime.Serialization

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

open Nessos.Vagabond
open Nessos.Vagabond.AssemblyProtocols
open Nessos.Vagabond.ExportableAssembly

open MBrace
open MBrace.Continuation
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.SampleRuntime

/// Actor publication utilities
type Actor =

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
//  Distributed read-only cell
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
//  Distributed Cancellation token sources
//

[<AutoOpen>]
module private CancellationTokenActor =

    type CancellationTokenSourceMsg =
        | IsCancellationRequested of IReplyChannel<bool>
        | RequestChild of childId:string * IReplyChannel<ActorRef<CancellationTokenSourceMsg> option>
        | Cancel

    let rec behaviour (state : Map<string, ActorRef<CancellationTokenSourceMsg>> option) (msg : CancellationTokenSourceMsg) = 
        async {
            match msg, state with
            | IsCancellationRequested rc, _ ->
                do! rc.Reply (Option.isNone state)
                return state
            // has been cancelled, do not return a child actor
            | RequestChild(id, rc), None ->
                return! rc.Reply None
                return state
            // cancellation token active, create a child actor
            | RequestChild(id, rc), Some children ->
                match children.TryFind id with
                | Some child -> 
                    // child with provided id alread exists, return that
                    do! rc.Reply (Some child)
                    return state
                | None ->
                    // child with provided id does not exist, create it
                    let newChild = createCancellationTokenActor()
                    do! rc.Reply (Some newChild)
                    let children2 = children.Add(id, newChild)
                    return (Some children2)
            // token is already canceled, nothing to do
            | Cancel, None -> return None
            // token canceled, cancel children and update state
            | Cancel, Some children ->
                for KeyValue(_,child) in children do
                    child <-- Cancel

                return None
        }

    and createCancellationTokenActor () =
        Actor.Stateful (Some Map.empty) behaviour
        |> Actor.Publish
        |> Actor.ref

type private CancellationTokenState =
    | Distributed of ActorRef<CancellationTokenSourceMsg> option // 'None' denotes a canceled token
    | Localized of parent:DistributedCancellationTokenSource

/// Defines a distributed cancellation token source that can be cancelled in the context of a distributed runtime.
and DistributedCancellationTokenSource private (id : string, state : CancellationTokenState) =

    // Distributed cancellation tokens can be initialized either as global actors or used for consumption within a local process
    // Local cancellation tokens still carry global semantics since they enforce a lazy 'globalization' scheme.
    // Attempting to serialize an instance will trigger this elevation mechanism and will result in an actor being created.

    static let localTokens = new System.Collections.Concurrent.ConcurrentDictionary<string, CancellationToken> ()
    /// creates a cancellation token that is updated by polling the cancellation token actor
    static let createLocalCancellationToken id (source : ActorRef<CancellationTokenSourceMsg>) =
        let ok, t = localTokens.TryGetValue id
        if ok then t
        elif source <!= IsCancellationRequested then
            new CancellationToken(canceled = true)
        else
            let createToken _ =
                let cts = new System.Threading.CancellationTokenSource()

                let rec checkCancellation () = async {
                    let! isCancelled = Async.Catch(source <!- IsCancellationRequested)
                    match isCancelled with
                    | Choice1Of2 true -> 
                        cts.Cancel()
                        localTokens.TryRemove id |> ignore
                    | Choice1Of2 false ->
                        do! Async.Sleep 200
                        return! checkCancellation ()
                    | Choice2Of2 e ->
                        do! Async.Sleep 1000
                        return! checkCancellation ()
                }

                do Async.Start(checkCancellation())
                cts.Token

            localTokens.AddOrUpdate(id, createToken, fun _ t -> t)

    // serializable state for the cancellatoin token
    let mutable state = state

    // nonserializable cancellation token source that is initialized only in case
    // of localized semantics
    [<NonSerialized>]
    let localCancellationTokenSource =
        match state with
        | Localized parentCts -> CancellationTokenSource.CreateLinkedTokenSource [| parentCts.LocalToken |]
        | _ -> null

    // nonserializable cancellation token bound to local process
    // can either be bound to local cancellation token source or remote actor polling loop
    [<NonSerialized>]
    let mutable localToken : CancellationToken option = None

    // lazily initializes the local cancellatoin token
    let getLocalCancellationToken () =
        match localToken with
        | Some ct -> ct
        | None ->
            lock state (fun () ->
                match state with
                | Localized parentCts ->
                    let ct = localCancellationTokenSource.Token
                    localToken <- Some ct
                    ct
                | Distributed None ->
                    let ct = new CancellationToken(canceled = true)
                    localToken <- Some ct
                    ct
                | Distributed (Some source) ->
                    let ct = createLocalCancellationToken id source
                    localToken <- Some ct
                    ct)
    
    /// Gets distributed actor cancellation token, elevating to distributed cts if necessary
    member private c.GetDistributedSource() =
        match state with
        | Distributed source -> source
        | Localized dcts ->
            // elevate parent to distributed source
            lock state (fun () ->
                let parentSource = dcts.GetDistributedSource()
                let source =
                    match parentSource with
                    | Some ps -> ps <!= fun ch -> RequestChild(id, ch)
                    | None -> None

                state <- Distributed source
                localToken <- None
                source)

    /// Triggers elevation in event of serialization
    [<OnSerializing>]
    member private c.ElevateCancellationToken (_ : StreamingContext) =
        c.GetDistributedSource() |> ignore

    /// Returns an actor ref that is subscribed to current parent actor
    member private c.RequestNewChildActor(childId : string) =
        let _ = c.GetDistributedSource() // elevate
        match state with
        | Distributed None -> None
        | Distributed (Some source) -> source <!= fun ch -> RequestChild(childId, ch)
        | Localized _ -> invalidOp "DistributedCancellationTokenSource : internal error."

    /// Creates a System.Threading.CancellationToken that is linked
    /// to the distributed cancellation token.
    member __.LocalToken = getLocalCancellationToken ()

    /// Force cancellation of the CTS
    member __.Cancel () = 
        lock state (fun () ->
            match state with
            | Localized _ -> 
                localCancellationTokenSource.Cancel()
                state <- Distributed None
            | Distributed (Some source) ->
                source <-- Cancel
                state <- Distributed None
            | Distributed None -> ())

    interface ICloudCancellationToken with
        member __.IsCancellationRequested = getLocalCancellationToken().IsCancellationRequested
        member __.LocalToken = getLocalCancellationToken()

    interface ICloudCancellationTokenSource with
        member __.Cancel () = __.Cancel()
        member __.Token = __ :> ICloudCancellationToken


    /// <summary>
    ///     Initializes a new distributed cancellation token source in the current process.
    /// </summary>
    /// <param name="parent">Linked parent cancellation token source.</param>
    static member Init() =
        let id = Guid.NewGuid().ToString()
        let actor = Some <| createCancellationTokenActor ()
        new DistributedCancellationTokenSource(id, Distributed actor)

    /// <summary>
    ///     Creates a linked cancellation token source with localized semantics.
    /// </summary>
    /// <param name="parent">Parent cancellation token source.</param>
    /// <param name="forceElevation">Force immediate elevation of cancellation token source. Defaults to false.</param>
    static member CreateLinkedCancellationTokenSource(parent : DistributedCancellationTokenSource, ?forceElevation : bool) =
        let id = Guid.NewGuid().ToString()
        let state =
            if defaultArg forceElevation false then
                Distributed <| parent.RequestNewChildActor id
            else
                Localized parent

        new DistributedCancellationTokenSource(id, state)


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
        | Cancelled e -> raise e

type private ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

/// Defines a reference to a distributed result cell instance.
type ResultCell<'T> private (id : string, source : ActorRef<ResultCellMsg<'T>>) as self =
    [<NonSerialized>]
    let mutable localCell : CacheAtom<Result<'T> option> option = None
    let getLocalCell() =
        match localCell with
        | Some c -> c
        | None ->
            lock self (fun () ->
                let cell = CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)
                localCell <- Some cell
                cell)

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

    interface ICloudTask<'T> with
        member c.Id = id
        member c.AwaitResult(?timeout:int) = local {
            let! r = Cloud.OfAsync <| Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value
        }

        member c.TryGetResult() = local {
            let! r = Cloud.OfAsync <| c.TryGetResult()
            return r |> Option.map (fun r -> r.Value)
        }

        member c.IsCompleted = 
            match getLocalCell().Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match getLocalCell().Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match getLocalCell().Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match getLocalCell().Value with
            | Some (Completed _) -> Tasks.TaskStatus.RanToCompletion
            | Some (Exception _) -> Tasks.TaskStatus.Faulted
            | Some (Cancelled _) -> Tasks.TaskStatus.Canceled
            | None -> Tasks.TaskStatus.Running

        member c.Result = 
            async {
                let! r = c.AwaitResult()
                return r.Value
            } |> Async.RunSync

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

        let id = Guid.NewGuid().ToString()
        new ResultCell<'T>(id, ref)

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

type private QueueMsg<'T, 'DequeueToken> =
    | EnQueue of 'T * (* fault count *) int
    | EnQueueMultiple of 'T []
    | TryDequeue of 'DequeueToken * IReplyChannel<('T * (* fault count *) int * LeaseMonitor) option>

type private ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
    static member Empty = new ImmutableQueue<'T>([],[])
    member __.Enqueue t = new ImmutableQueue<'T>(front, t :: back)
    member __.EnqueueMultiple ts = new ImmutableQueue<'T>(front, List.rev ts @ back)
    member __.TryDequeue () = 
        match front with
        | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, back))
        | [] -> 
            match List.rev back with
            | [] -> None
            | hd :: tl -> Some(hd, new ImmutableQueue<'T>(tl, []))

/// Provides a distributed, fault-tolerant queue implementation
type Queue<'T, 'DequeueToken> private (source : ActorRef<QueueMsg<'T, 'DequeueToken>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue (t, 0)
    member __.EnqueueMultiple (ts : 'T []) = source <-- EnQueueMultiple ts
    member __.TryDequeue (token) = source <!- fun ch -> TryDequeue(token, ch)

    /// Initializes a new distribued queue instance.
    static member Init(shouldDequeue : 'DequeueToken -> 'T -> bool) =
        let self = ref Unchecked.defaultof<ActorRef<QueueMsg<'T, 'DequeueToken>>>
        let behaviour (queue : ImmutableQueue<'T * int>) msg = async {
            match msg with
            | EnQueue (t, faultCount) -> return queue.Enqueue (t, faultCount)
            | EnQueueMultiple ts -> return ts |> Seq.map (fun t -> (t,0)) |> Seq.toList |> queue.EnqueueMultiple
            | TryDequeue (dt, rc) ->
                match queue.TryDequeue() with
                | Some((t, faultCount), queue') when (try shouldDequeue dt t with _ -> false) ->
                    let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                    do! rc.Reply (Some (t, faultCount, leaseMonitor))
                    let _ = putBack.Subscribe(fun () -> self.Value <-- EnQueue (t, faultCount + 1))
                    return queue'

                | _ ->
                    do! rc.Reply None
                    return queue
        }

        self :=
            Actor.Stateful ImmutableQueue<'T * int>.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new Queue<'T, 'DequeueToken>(self.Value)




//
//  Distributed atom implementation
//

type private tag = uint64

type private AtomMsg<'T> =
    | GetValue of IReplyChannel<tag * 'T>
    | TrySetValue of tag * 'T * IReplyChannel<bool>
    | ForceValue of 'T * IReplyChannel<unit>
    | Dispose of IReplyChannel<unit>

type Atom<'T> private (id : string, source : ActorRef<AtomMsg<'T>>) =

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = Cloud.OfAsync <| async {
            let! _,value = source <!- GetValue
            return value
        }

        member __.Dispose() = Cloud.OfAsync <| async { return! source <!- Dispose }

        member __.Force(value : 'T) = Cloud.OfAsync <| async { return! source <!- fun ch -> ForceValue(value, ch) }
        member __.Update(f : 'T -> 'T, ?maxRetries) = Cloud.OfAsync <| async {
            if maxRetries |> Option.exists (fun i -> i < 0) then
                invalidArg "maxRetries" "must be non-negative."

            let rec tryUpdate retries = async {
                let! tag, value = source <!- GetValue
                let value' = f value
                let! success = source <!- fun ch -> TrySetValue(tag, value', ch)
                if success then return ()
                else
                    match maxRetries with
                    | None -> return! tryUpdate None
                    | Some 0 -> return raise <| new OperationCanceledException("ran out of retries.")
                    | Some i -> return! tryUpdate (Some (i-1))
            }

            return! tryUpdate maxRetries
        }

    static member Init(id : string, init : 'T) =
        let behaviour (state : (uint64 * 'T) option) (msg : AtomMsg<'T>) = async {
            match state with
            | None -> // object disposed
                let e = new System.ObjectDisposedException("ActorAtom")
                match msg with
                | GetValue rc -> do! rc.ReplyWithException e
                | TrySetValue(_,_,rc) -> do! rc.ReplyWithException e
                | ForceValue(_,rc) -> do! rc.ReplyWithException e
                | Dispose rc -> do! rc.ReplyWithException e
                return state

            | Some ((tag, value) as s) ->
                match msg with
                | GetValue rc ->
                    do! rc.Reply s
                    return state
                | TrySetValue(tag', value', rc) ->
                    if tag' = tag then
                        do! rc.Reply true
                        return Some (tag + 1uL, value')
                    else
                        do! rc.Reply false
                        return state
                | ForceValue(value', rc) ->
                    do! rc.Reply ()
                    return Some (tag + 1uL, value')
                | Dispose rc ->
                    do! rc.Reply ()
                    return None
        }

        let ref =
            Actor.Stateful (Some (0uL, init)) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Atom<'T>(id, ref)
        
            

//
//  Distributed channel implementation
//

type private ChannelMsg<'T> =
    | Send of 'T
    | Receive of IReplyChannel<'T>

type Channel<'T> private (id : string, source : ActorRef<ChannelMsg<'T>>) =

    interface IReceivePort<'T> with
        member __.Id = id
        member __.Receive(?timeout : int) = Cloud.OfAsync <| async { return! source.PostWithReply(Receive, ?timeout = timeout) }
        member __.Dispose () = local.Zero()

    interface ISendPort<'T> with
        member __.Id = id
        member __.Send(msg : 'T) =  Cloud.OfAsync <| async { return! source.AsyncPost(Send msg) }

    /// Initializes a new distributed queue instance.
    static member Init(id : string) =
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

        new Channel<'T>(id, self.Value)

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
    member __.RequestCancellationTokenSource() = __.RequestResource(fun () -> DistributedCancellationTokenSource.Init())
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())
    member __.RequestChannel<'T>(id) = __.RequestResource(fun () -> Channel<'T>.Init(id))
    member __.RequestAtom<'T>(id, init) = __.RequestResource(fun () -> Atom<'T>.Init(id, init))

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
// Assembly exporter : provides assembly uploading facility for Vagabond
//

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<ExportableAssembly list> 

/// Provides assembly uploading facility for Vagabond.
type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let vas = VagabondRegistry.Instance.GetVagabondAssemblies(ids)
            let packages = VagabondRegistry.Instance.CreateRawAssemblies vas
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
                    member __.PullAssemblies ids = async {
                        let! eas = exporter <!- fun ch -> RequestAssemblies(ids, ch)
                        return VagabondRegistry.Instance.CacheRawAssemblies(eas)
                    }
            }

        do! VagabondRegistry.Instance.ReceiveDependencies publisher
    }

    /// <summary>
    ///     Compute assembly dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be analyzed</param>
    member __.ComputeDependencies (graph:'T) =
        VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Vagabond.ComputeAssemblyId


type WorkerManager =
    | SubscribeToRuntime of IReplyChannel<unit> * string * int
    | Unsubscribe of IReplyChannel<unit>
