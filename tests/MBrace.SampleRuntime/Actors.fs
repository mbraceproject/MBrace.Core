module Nessos.MBrace.SampleRuntime.Actors

open System

open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol

type Actor private () =
    static do System.Threading.ThreadPool.SetMinThreads(100, 100) |> ignore
    static do TcpListenerPool.RegisterListener(IPEndPoint.any)
    static let endPoint = 
        let listener = TcpListenerPool.GetListeners(IPEndPoint.any) |> Seq.head
        listener.LocalEndPoint

    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ new Unidirectional.UTcp() ] 
        |> Actor.start
        |> Actor.ref

    static member LocalEndPoint = endPoint

//
//  Disposable actor
//

type Disposable<'Msg> = Msg of 'Msg | Dispose

[<AbstractClass>]
type DisposableActor<'Msg>(actorRef : ActorRef<Disposable<'Msg>>) =
    member __.Ref = actorRef
    member __.Dispose () = actorRef <-- Dispose
    interface IDisposable with
        member __.Dispose() = actorRef <-- Dispose

module Disposable =
    let stateful init (f : 'State -> 'Msg -> Async<'State>) = 
        let rec loop state (self : Actor<Disposable<'Msg>>) = async {
            let! dmsg = self.Receive()
            match dmsg with
            | Msg msg -> let! state' = f state msg in return! loop state' self
            | Dispose -> return ()
        }

        Actor.bind (loop init)

    let stateless (f : 'Msg -> Async<unit>) = stateful () (fun () m -> f m)

//
//  Distributed latch implementation
//

type LatchMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

type Latch private (source : ActorRef<_>) =
    inherit DisposableActor<LatchMessage>(source)
    member __.Increment() = source <!= (Msg << Increment)
    member __.Value = source <!= (Msg << GetValue)

    static member Init(init : int) =
        let behaviour count msg = async {
            match msg with
            | Increment rc ->
                do rc.Reply <| Value (count + 1)
                return count + 1
            | GetValue rc ->
                do rc.Reply <| Value count
                return count
        }

        let ref = Disposable.stateful 0 behaviour |> Actor.Publish
        new Latch(ref)

//
//  Distributed resource aggregator
//

type ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * completed:IReplyChannel<bool>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

type ResultAggregator<'T> private (source : ActorRef<_>) =
    inherit DisposableActor<ResultAggregatorMsg<'T>> (source)
    member __.SetResult(index : int, value : 'T) = source <!= fun ch -> Msg(SetResult(index, value, ch))
    member __.IsCompleted = source <!= (Msg << IsCompleted)
    member __.ToArray () = source <!= (Msg << ToArray)

    static member Init(size : int) =
        let behaviour ((results, count) as state : 'T [] * int) msg = async {
            match msg with
            | SetResult(idx, value, ch) -> 
                // should check if idx has been already assigned...
                results.[idx] <- value
                ch.Reply <| Value(count + 1 = size)
                return (results, count + 1)

            | IsCompleted rc ->
                rc.Reply <| Value (count = size)
                return state

            | ToArray rc ->
                rc.Reply <| Value results
                return state
        }

        let buf = Array.zeroCreate<'T> size
        let ref = Disposable.stateful (buf,0) behaviour |> Actor.Publish
        new ResultAggregator<'T>(ref)

//
//  Distributed result cell
//

type Result<'T> =
    | Completed of 'T
    | Exception of exn
    | Cancelled of exn
with
    member r.Value =
        match r with
        | Completed t -> t
        | Exception e -> raise e
        | Cancelled e -> raise e 

type ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

type ResultCell<'T> private (source : ActorRef<_>) =
    inherit DisposableActor<ResultCellMsg<'T>>(source)
    member c.SetResult result = source <!= fun ch -> Msg(SetResult(result, ch))
    member c.TryGetResult () = source <!= (Msg << TryGetResult)
    member c.AwaitResult() = async {
        let! result = source <!- (Msg << TryGetResult)
        match result with
        | None -> return! c.AwaitResult()
        | Some r -> return r
    }

    static member Init() : ResultCell<'T> =
        let behavior state msg = async {
            match msg with
            | SetResult (_, rc) when Option.isSome state -> 
                rc.Reply <| Value false
                return state

            | SetResult (result, rc) ->
                rc.Reply <| Value true
                return Some result

            | TryGetResult rc ->
                rc.Reply <| Value state
                return state
        }

        let ref = Disposable.stateful None behavior |> Actor.Publish
        new ResultCell<'T>(ref)

//
//  Distributed Cancellation token
//

type CancellationTokenId = string

type CancellationTokenManagerMsg =
    | RequestCancellationTokenSource of parent:CancellationTokenId option * IReplyChannel<CancellationTokenId>
    | IsCancellationRequested of id:CancellationTokenId * IReplyChannel<bool>
    | Cancel of id:CancellationTokenId

type DistributedCancellationTokenSource = 
    internal {
        Id : CancellationTokenId
        Source : ActorRef<Disposable<CancellationTokenManagerMsg>>
    }
with
    member ct.Cancel () = ct.Source <-- Msg(Cancel ct.Id)
    member ct.IsCancellationRequested = ct.Source <!= fun ch -> Msg(IsCancellationRequested(ct.Id, ch))
    member ct.GetLocalCancellationToken() =
        let cts = new System.Threading.CancellationTokenSource()

        let rec checkCancellation () = async {
            let! isCancelled = ct.Source <!- fun ch -> Msg(IsCancellationRequested(ct.Id, ch))
            if isCancelled then
                cts.Cancel()
                return ()
            else
                do! Async.Sleep 200
                return! checkCancellation ()
        }

        do Async.Start(checkCancellation())

        cts.Token

type CancellationTokenManager private (source : ActorRef<_>) =
    inherit DisposableActor<CancellationTokenManagerMsg>(source)
    member __.RequestCancellationTokenSource(?parent : DistributedCancellationTokenSource) =
        let ids = parent |> Option.map (fun p -> p.Id)
        let newId = source <!= fun ch -> Msg(RequestCancellationTokenSource(ids, ch))
        { Id = newId ; Source = source }

    static member Init() =
        let behavior (state : Map<CancellationTokenId, bool * CancellationTokenId list>) msg = async {
            match msg with
            | RequestCancellationTokenSource (parent, rc) ->
                let newId = Guid.NewGuid().ToString()
                let state =
                    match parent |> Option.bind state.TryFind with
                    | None -> state.Add(newId, (false, []))
                    | Some(isCancelled, children) ->
                        state.Add(parent.Value, (isCancelled, newId :: children))
                             .Add(newId, (isCancelled, []))

                rc.Reply <| Value newId
                return state

            | IsCancellationRequested (id, rc) ->
                let isCancelled = state.TryFind id |> Option.exists fst
                rc.Reply <| Value isCancelled
                return state

            | Cancel id ->
                let rec traverseCancellation 
                        (state : Map<CancellationTokenId, bool * CancellationTokenId list>) 
                        (remaining : CancellationTokenId list) = 

                    match remaining with
                    | [] -> state
                    | id :: tail ->
                        match state.TryFind id with
                        | None 
                        | Some(true,_) -> traverseCancellation state tail
                        | Some(false, children) -> 
                            traverseCancellation (Map.add id (true, children) state) (children @ tail)

                return traverseCancellation state [id]
        }

        let ref = Disposable.stateful Map.empty behavior |> Actor.Publish
        new CancellationTokenManager(ref)

//
//  Distributed queue implementation
//

type QueueMsg<'T> =
    | EnQueue of 'T
    | TryDequeue of IReplyChannel<'T option>

type Queue<'T> private (source : ActorRef<_>) =
    inherit DisposableActor<QueueMsg<'T>>(source)
    member __.Enqueue (t : 'T) = source <-- Msg (EnQueue t)
    member __.TryDequeue () = source <!= (Msg << TryDequeue)

    static member Init() =
        let queue = System.Collections.Generic.Queue<'T> ()
        let behaviour msg = async {
            match msg with
            | EnQueue t -> queue.Enqueue t
            | TryDequeue rc when queue.Count = 0 -> rc.Reply <| Value None
            | TryDequeue rc ->
                let t = queue.Dequeue()
                rc.Reply <| Value (Some t)
        }

        let ref = Disposable.stateless behaviour |> Actor.Publish
        new Queue<'T>(ref)


//
//  Distributed Resource factory
//

type ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

type ResourceFactory private (source : ActorRef<_>) =
    inherit DisposableActor<ResourceFactoryMsg>(source)
    member __.RequestResource<'T>(factory : unit -> 'T) =
        let ctor () = factory () :> obj
        let res = source <!= fun ch -> Msg(RequestResource(ctor, ch))
        res :?> 'T

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())

    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = ctor ()
            rc.Reply <| Value r
        }

        let ref = Disposable.stateless behavior |> Actor.Publish
        new ResourceFactory(ref)


//
//  Distributed event
//

type EventActor<'T> private (ref : ActorRef<_>) =
    inherit DisposableActor<'T>(ref)
    member __.Trigger t = ref <-- Msg t

    static member Init() =
        let event = new Event<'T> ()
        let behavior t = async { return try event.Trigger t with _ -> () }
        let ref = Disposable.stateless behavior |> Actor.Publish
        event.Publish, new EventActor<'T>(ref)