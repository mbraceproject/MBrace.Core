module Nessos.MBrace.Remote.Gadgets

open Nessos.Thespian

open Nessos.MBrace.Remote.Thespian

//
//  Distributed latch implementation
//

type private LatchMessage =
    | Increment of IReplyChannel<int>

type Latch private (source : ActorRef<LatchMessage>) =
    member __.Increment() = source <!= Increment

    static member Init(init : int) =
        let behaviour count (Increment rc) = async {
            do rc.Reply <| Value (count + 1)
            return (count + 1)
        }

        let actor =
            Behavior.stateful init behaviour
            |> Actor.bind
            |> Actor.publish [ defaultProtocol ]
            |> Actor.start

        new Latch(actor.Ref)

//
//  Distributed resource aggregator
//

type private ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * completed:IReplyChannel<bool>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

type ResultAggregator<'T> private (source : ActorRef<ResultAggregatorMsg<'T>>) =
    
    member __.SetResult(index : int, value : 'T) =
        source <!= fun ch -> SetResult(index, value, ch)

    member __.IsCompleted = source <!= IsCompleted

    member __.ToArray () = source <!= ToArray

    static member Init(size : int) =
        let behaviour ((results : 'T [], count) as state) msg = async {
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

        let actor =
            Behavior.stateful (buf,0) behaviour
            |> Actor.bind
            |> Actor.publish [ defaultProtocol ]
            |> Actor.start

        new ResultAggregator<'T>(actor.Ref)


//
//  Distributed queue implementation
//

type private QueueMsg<'T> =
    | EnQueue of 'T
    | TryDequeue of IReplyChannel<'T option>

type Queue<'T> private (source : ActorRef<QueueMsg<'T>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue t
    member __.TryDequeue () = source <!= TryDequeue

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

        let actor =
            Behavior.stateless behaviour
            |> Actor.bind
            |> Actor.publish [ defaultProtocol ]
            |> Actor.start

        new Queue<'T>(actor.Ref)


type private ResourceManagerMsg =
    | RequestLatch of IReplyChannel<Latch>
    | RequestResultAggregator of ctor:(unit -> obj) * IReplyChannel<obj>

type ResourceManager private (source : ActorRef<ResourceManagerMsg>) =
    member __.RequestLatch() = source <!= RequestLatch
    member __.RequestResultAggregator<'T>(count : int) =
        // existentially pack the type variable in a constructor lambda
        let ctor () = ResultAggregator<'T>.Init(count) :> obj
        let res = source <!= fun ch -> RequestResultAggregator(ctor, ch)
        res :?> ResultAggregator<'T>

    static member Init () =
        let behavior msg = async {
            match msg with
            | RequestLatch rc -> 
                let l = Latch.Init(0)
                rc.Reply <| Value l
            | RequestResultAggregator(ctor, rc) ->
                rc.Reply <| Value(ctor ())
        }

        let actor =
            Behavior.stateless behavior
            |> Actor.bind
            |> Actor.publish [ defaultProtocol ]
            |> Actor.start

        new ResourceManager(actor.Ref)