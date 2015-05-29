namespace MBrace.SampleRuntime.Actors

//
//  Implements a collection of distributed resources that provide
//  coordination for execution in the distributed runtime.
//  The particular implementations are done using Thespian,
//  a distributed actor framework for F#.
//

open System
open System.Threading
open System.Collections.Generic
open System.Runtime.Serialization

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.SampleRuntime

/// Actor publication utilities
type Actor =

    /// Publishes an actor instance to the default TCP protocol
    static member Publish(actor : Actor<'T>) =
        ignore Config.Serializer
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

type private CounterMessage =
    | IncreaseBy of int * IReplyChannel<int>
    | GetValue of IReplyChannel<int>

/// Distributed counter implementation
type Counter private (source : ActorRef<CounterMessage>) =
    interface ICloudCounter with
        member __.Increment () = source <!- fun ch -> IncreaseBy(1,ch)
        member __.Decrement () = source <!- fun ch -> IncreaseBy(-1,ch)
        member __.Value = source <!- GetValue
        member __.Dispose () = async.Zero()

    /// Initialize a new latch instance in the current process
    static member Init(init : int) =
        let behaviour count msg = async {
            match msg with
            | IncreaseBy (i, rc) ->
                do! rc.Reply (count + i)
                return (count + i)
            | GetValue rc ->
                do! rc.Reply count
                return count
        }

        let ref =
            Actor.Stateful init behaviour
            |> Actor.Publish
            |> Actor.ref

        new Counter(ref)

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
    | SetResult of index:int * value:'T * overwrite:bool * completed:IReplyChannel<bool>
    | GetCompleted of IReplyChannel<int>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

/// A distributed resource that aggregates an array of results.
type ResultAggregator<'T> private (capacity : int, source : ActorRef<ResultAggregatorMsg<'T>>) =
    interface IResultAggregator<'T> with
        member __.Capacity = capacity
        member __.CurrentSize = source <!- GetCompleted
        member __.IsCompleted = source <!- IsCompleted
        member __.SetResult(index : int, value : 'T, overwrite : bool) = source <!- fun ch -> SetResult(index, value, overwrite, ch)
        member __.ToArray () = source <!- ToArray
        member __.Dispose () = async.Zero()

    /// Initializes a result aggregator of given size at the current process.
    static member Init(size : int) =
        let behaviour (results : Map<int, 'T>) msg = async {
            match msg with
            | SetResult(i, value, _, rc) when i < 0 || i >= size ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, false, rc) when results.ContainsKey i ->
                let e = new InvalidOperationException(sprintf "result at position '%d' has already been set." i)
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, _, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = size
                do! rc.Reply isCompleted
                return results

            | GetCompleted rc ->
                do! rc.Reply results.Count
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

        new ResultAggregator<'T>(size, ref)

//
//  Distributed Cancellation token sources
//

type private CancellationEntryMsg =
    | IsCancellationRequested of IReplyChannel<bool>
    | RequestChild of IReplyChannel<CancellationEntry option>
    | Cancel

and CancellationEntry private (id : string, source : ActorRef<CancellationEntryMsg>) =
    member __.Id = id
    member __.Cancel () = source.AsyncPost Cancel
    member __.IsCancellationRequested = source <!- IsCancellationRequested
    member __.TryCreateChild () = source <!- RequestChild
    
    interface ICancellationEntry with
        member __.UUID = id
        member __.Cancel() = __.Cancel()
        member __.IsCancellationRequested = __.IsCancellationRequested
        member __.Dispose () = async.Zero()

    static member Init() =
        let behaviour (state : Map<string, CancellationEntry> option) (msg : CancellationEntryMsg) = async {
            match msg, state with
            | IsCancellationRequested rc, _ ->
                do! rc.Reply (Option.isNone state)
                return state
            // has been cancelled, do not return a child actor
            | RequestChild rc, None ->
                return! rc.Reply None
                return state
            // cancellation token active, create a child actor
            | RequestChild rc, Some children ->
                let child = CancellationEntry.Init()
                do! rc.Reply (Some child)
                return Some <| children.Add(child.Id, child)

            // token is already canceled, nothing to do
            | Cancel, None -> return None
            // token canceled, cancel children and update state
            | Cancel, Some children ->
                do! 
                    children
                    |> Seq.map (fun kv -> kv.Value.Cancel())
                    |> Async.Parallel
                    |> Async.Ignore

                return None
        }

        let id = mkUUID()
        let aref =
            Actor.Stateful (Some Map.empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new CancellationEntry(id, aref)

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
type TaskCompletionSource<'T> private (id : string, source : ActorRef<ResultCellMsg<'T>>) as self =
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

    interface ICloudTaskCompletionSource<'T> with
        member x.Dispose(): Async<unit> = async.Zero()

        member x.SetCompleted(t : 'T): Async<unit> = 
            x.SetResult(Completed t) |> Async.Ignore
        
        member x.SetException(edi: ExceptionDispatchInfo): Async<unit> = 
            x.SetResult(Exception edi) |> Async.Ignore
        
        member x.SetCancelled(exn: OperationCanceledException): Async<unit> =
            x.SetResult(Cancelled exn) |> Async.Ignore
        
        member x.Task: ICloudTask<'T> = x :> _
        

    /// Initialize a new result cell in the local process
    static member Init() : TaskCompletionSource<'T> =
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
        new TaskCompletionSource<'T>(id, ref)

//
//  CloudDictionary implementation
//

type private CloudDictionaryMsg<'T> =
    | Add of key:string * value:'T * force:bool * IReplyChannel<bool>
    | AddOrUpdate of key:string * updater:('T option -> 'T) * IReplyChannel<'T>
    | Update of key:string * updater:('T -> 'T) * IReplyChannel<'T>
    | ContainsKey of key:string * IReplyChannel<bool>
    | Remove of key:string * IReplyChannel<bool>
    | TryFind of key:string * IReplyChannel<'T option>
    | GetCount of IReplyChannel<int64>
    | ToArray of IReplyChannel<KeyValuePair<string, 'T> []>

type CloudDictionary<'T> private (id : string, source : ActorRef<CloudDictionaryMsg<'T>>) =
    let (<!-) ar msgB = local { return! Cloud.OfAsync(ar <!- msgB)}
    interface ICloudDictionary<'T> with
        member x.Add(key: string, value: 'T): Local<unit> = 
            local { let! _ = source <!- fun ch -> Add(key, value, true, ch) in return () }
        
        member x.AddOrUpdate(key: string, updater: 'T option -> 'T): Local<'T> = 
            source <!- fun ch -> AddOrUpdate(key, updater, ch)

        member x.Update(key : string, updater : 'T -> 'T) : Local<'T> =
            source <!- fun ch -> Update(key, updater, ch)
        
        member x.ContainsKey(key: string): Local<bool> = 
            source <!- fun ch -> ContainsKey(key, ch)

        member x.IsKnownSize = true
        member x.IsKnownCount = true
        
        member x.Count: Local<int64> = 
            source <!- GetCount

        member x.Size : Local<int64> =
            source <!- GetCount
        
        member x.Dispose(): Local<unit> = local.Zero ()
        
        member x.Id: string = id
        
        member x.Remove(key: string): Local<bool> = 
            source <!- fun ch -> Remove(key, ch)
        
        member x.ToEnumerable() = local {
            let! pairs = source <!- ToArray
            return pairs :> seq<_>
        }
        
        member x.TryAdd(key: string, value: 'T): Local<bool> = 
            source <!- fun ch -> Add(key, value, false, ch)
        
        member x.TryFind(key: string): Local<'T option> = 
            source <!- fun ch -> TryFind(key, ch)

    static member Init() =
        let behaviour (state : Map<string, 'T>) (msg : CloudDictionaryMsg<'T>) = async {
            match msg with
            | Add(key, value, false, rc) when state.ContainsKey key ->
                do! rc.Reply false
                return state
            | Add(key, value, _, rc) ->
                do! rc.Reply true
                return state.Add(key, value)
            | AddOrUpdate(key, updater, rc) ->
                let t = updater (state.TryFind key)
                do! rc.Reply t
                return state.Add(key, t)
            | Update(key, updater, rc) ->
                match state.TryFind key with
                | None ->
                    do! rc.ReplyWithException(new KeyNotFoundException(key))
                    return state
                | Some t ->
                    do! rc.Reply t
                    return state.Add(key, t)

            | ContainsKey(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state
            | Remove(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state.Remove key
            | TryFind(key, rc) ->
                do! rc.Reply (state.TryFind key)
                return state
            | GetCount rc ->
                do! rc.Reply (int64 state.Count)
                return state
            | ToArray rc ->
                do! rc.Reply (state |> Seq.toArray)
                return state
        }

        let id = Guid.NewGuid().ToString()
        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new CloudDictionary<'T>(id, ref)

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
        member __.Transact(f : 'T -> 'R * 'T, ?maxRetries) = Cloud.OfAsync <| async {
            if maxRetries |> Option.exists (fun i -> i < 0) then
                invalidArg "maxRetries" "must be non-negative."

            let cell = ref Unchecked.defaultof<'R>
            let rec tryUpdate retries = async {
                let! tag, value = source <!- GetValue
                let r, value' = f value
                cell := r
                let! success = source <!- fun ch -> TrySetValue(tag, value', ch)
                if success then return ()
                else
                    match maxRetries with
                    | None -> return! tryUpdate None
                    | Some 0 -> return raise <| new OperationCanceledException("ran out of retries.")
                    | Some i -> return! tryUpdate (Some (i-1))
            }

            do! tryUpdate maxRetries
            return cell.Value
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

    member __.RequestCounter(count) = __.RequestResource(fun () -> Counter.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestCancellationEntry() = __.RequestResource(fun () -> CancellationEntry.Init())
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> TaskCompletionSource<'T>.Init())
    member __.RequestChannel<'T>(id) = __.RequestResource(fun () -> Channel<'T>.Init(id))
    member __.RequestDictionary<'T>() = __.RequestResource(fun () -> CloudDictionary<'T>.Init())
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

type WorkerManager =
    | SubscribeToRuntime of IReplyChannel<unit> * string * int
    | Unsubscribe of IReplyChannel<unit>
