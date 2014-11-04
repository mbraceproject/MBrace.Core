namespace Nessos.MBrace.SampleRuntime.Actors

open System
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols

open Nessos.Vagrant

open Nessos.MBrace.Runtime
open Nessos.MBrace.SampleRuntime

type Actor private () =
    static do Config.initRuntimeState()

    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.btcp() ] 
        |> Actor.start
        |> Actor.ref

//
//  Distributed latch implementation
//

type private LatchMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

type Latch private (source : ActorRef<LatchMessage>) =
    member __.Increment () = source <!- Increment
    member __.Value = source <!= GetValue

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
            Behavior.stateful init behaviour
            |> Actor.bind
            |> Actor.Publish

        new Latch(ref)

//
//  Distributed resource aggregator
//

type private ResultAggregatorMsg<'T> =
    | SetResult of index:int * value:'T * completed:IReplyChannel<bool>
    | IsCompleted of IReplyChannel<bool>
    | ToArray of IReplyChannel<'T []>

type ResultAggregator<'T> private (source : ActorRef<ResultAggregatorMsg<'T>>) =
    
    member __.SetResult(index : int, value : 'T) =
        source <!- fun ch -> SetResult(index, value, ch)

    member __.ToArray () = source <!- ToArray

    static member Init(size : int) =
        let behaviour (state : Map<int, 'T>) msg = async {
            match msg with
            | SetResult(i, _, rc) when state.ContainsKey i -> 
                let err = new InvalidOperationException(sprintf "Index %d already assigned." i)
                do! rc.ReplyWithException err
                return state

            | SetResult(i, value, rc) ->
                let state' = state.Add(i, value)
                do! rc.Reply ((state'.Count = size))
                return state'

            | IsCompleted rc ->
                do! rc.Reply ((state.Count = size))
                return state

            | ToArray rc ->
                do! rc.Reply (state |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray)
                return state
        }

        let ref =
            Behavior.stateful Map.empty behaviour
            |> Actor.bind
            |> Actor.Publish

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

type private ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

type ResultCell<'T> private (source : ActorRef<ResultCellMsg<'T>>) =
    member c.SetResult result = source <!- fun ch -> SetResult(result, ch)
    member c.TryGetResult () = source <!- TryGetResult
    member c.AwaitResult() = async {
        let! result = source <!- TryGetResult
        match result with
        | None -> 
            do! Async.Sleep 100
            return! c.AwaitResult()
        | Some r -> return r
    }

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
            Behavior.stateful None behavior
            |> Actor.bind
            |> Actor.Publish

        new ResultCell<'T>(ref)

//
//  Distributed Cancellation token
//

type internal CancellationTokenId = string

type internal CancellationTokenManagerMsg =
    | RequestCancellationTokenSource of parent:CancellationTokenId option * IReplyChannel<CancellationTokenId>
    | IsCancellationRequested of id:CancellationTokenId * IReplyChannel<bool>
    | Cancel of id:CancellationTokenId

type DistributedCancellationTokenSource internal (id : CancellationTokenId, source : ActorRef<CancellationTokenManagerMsg>) =
    member __.Id = id
    member ct.Cancel () = source <-- Cancel id
    member ct.IsCancellationRequested = source <!- fun ch -> IsCancellationRequested(id, ch)
    member ct.GetLocalCancellationToken() =
        let cts = new System.Threading.CancellationTokenSource()

        let rec checkCancellation () = async {
            let! isCancelled = Async.Catch(source <!- fun ch -> IsCancellationRequested(id, ch))
            match isCancelled with
            | Choice1Of2 true -> cts.Cancel()
            | Choice1Of2 false ->
                do! Async.Sleep 100
                return! checkCancellation ()
            | Choice2Of2 e ->
                do! Async.Sleep 1000
                return! checkCancellation ()
        }

        do Async.Start(checkCancellation())
        cts.Token

type CancellationTokenManager private (source : ActorRef<CancellationTokenManagerMsg>) =
    member __.RequestCancellationTokenSource(?parent : DistributedCancellationTokenSource) = async {
        let ids = parent |> Option.map (fun p -> p.Id)
        let! newId = source <!- fun ch -> RequestCancellationTokenSource(ids, ch)
        return new DistributedCancellationTokenSource(newId, source)
    }

    static member Init() =
        let behavior (state : Map<CancellationTokenId, CancellationTokenId list>) msg = async {
            match msg with
            | RequestCancellationTokenSource (parent, rc) ->
                let newId = Guid.NewGuid().ToString()
                let state =
                    match parent with
                    | None -> state.Add(newId, [])
                    | Some p ->
                        match state.TryFind p with
                        | None -> state
                        | Some children -> state.Add(p, newId :: children).Add(newId, [])

                do! rc.Reply newId
                return state

            | IsCancellationRequested (id, rc) ->
                let isCancelled = not <| state.ContainsKey id
                do! rc.Reply isCancelled
                return state

            | Cancel id ->
                let rec traverseCancellation 
                        (state : Map<CancellationTokenId, CancellationTokenId list>) 
                        (remaining : CancellationTokenId list) = 

                    match remaining with
                    | [] -> state
                    | id :: tail ->
                        match state.TryFind id with
                        | None -> traverseCancellation state tail
                        | Some children -> traverseCancellation (Map.remove id state) (children @ tail)

                return traverseCancellation state [id]
        }

        let ref =
            Behavior.stateful Map.empty behavior
            |> Actor.bind
            |> Actor.Publish

        new CancellationTokenManager(ref)

//
//  Distributed queue implementation
//

type private QueueMsg<'T> =
    | EnQueue of 'T
    | TryDequeue of IReplyChannel<'T option>

type private ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
    static member Empty = new ImmutableQueue<'T>([],[])
    member __.Enqueue t = new ImmutableQueue<'T>(front, t :: back)
    member __.TryDequeue () =
        match front with
        | t :: tl -> Some(t, new ImmutableQueue<'T>(tl, back))
        | [] ->
            match List.rev back with
            | [] -> None
            | t :: tl -> Some(t, new ImmutableQueue<'T>(tl, []))

type Queue<'T> private (source : ActorRef<QueueMsg<'T>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue t
    member __.TryDequeue () = source <!- TryDequeue

    static member Init() =
        let behaviour (queue : ImmutableQueue<'T>) msg = async {
            match msg with
            | EnQueue t -> return queue.Enqueue t
            | TryDequeue rc ->
                match queue.TryDequeue () with
                | None ->
                    do! rc.Reply None
                    return queue

                | Some(t, queue') ->
                    do! rc.Reply (Some t)
                    return queue'
        }

        let ref =
            Behavior.stateful ImmutableQueue<'T>.Empty behaviour
            |> Actor.bind
            |> Actor.Publish

        new Queue<'T>(ref)


//
//  Distributed Resource factory
//

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =
    member __.RequestResource<'T>(factory : unit -> 'T) = async {
        let ctor () = factory () :> obj
        let! resource = source <!- fun ch -> RequestResource(ctor, ch)
        return resource :?> 'T
    }

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())

    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = try ctor () |> Choice1Of2 with e -> Choice2Of2 e
            match r with
            | Choice1Of2 res -> do! rc.Reply res
            | Choice2Of2 e -> do! rc.ReplyWithException e
        }

        let ref =
            Behavior.stateless behavior
            |> Actor.bind
            |> Actor.Publish

        new ResourceFactory(ref)


// assembly exporter

type private AssemblyExporterMsg =
    | RequestAssemblies of AssemblyId list * IReplyChannel<AssemblyPackage list> 

type AssemblyExporter private (exporter : ActorRef<AssemblyExporterMsg>) =
    static member Init() =
        let behaviour (RequestAssemblies(ids, ch)) = async {
            let packages = VagrantRegistry.Vagrant.CreateAssemblyPackages(ids, includeAssemblyImage = true)
            do! ch.Reply packages
        }

        let ref = 
            Behavior.stateless behaviour 
            |> Actor.bind 
            |> Actor.Publish

        new AssemblyExporter(ref)

    member __.LoadDependencies(ids : AssemblyId list) = async {
        let publisher =
            {
                new IRemoteAssemblyPublisher with
                    member __.GetRequiredAssemblyInfo () = async { return ids }
                    member __.PullAssemblies ids = exporter <!- fun ch -> RequestAssemblies(ids, ch)
            }

        do! VagrantRegistry.Vagrant.ReceiveDependencies publisher
    }

    member __.ComputeDependencies (graph:'T) =
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId