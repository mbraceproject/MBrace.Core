module Nessos.MBrace.SampleRuntime.Actors

open System
open System.Reflection

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Vagrant

open Nessos.MBrace.Runtime

type Actor private () =
    static do
        let ignoredAssemblies =
            let this = Assembly.GetExecutingAssembly()
            let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
            new System.Collections.Generic.HashSet<_>(dependencies)

        VagrantRegistry.Initialize(ignoreAssembly = ignoredAssemblies.Contains, loadPolicy = AssemblyLoadPolicy.ResolveAll)

    static do
        let serializer = new Nessos.Thespian.Serialization.FsPicklerMessageSerializer(VagrantRegistry.Pickler)
        Nessos.Thespian.Serialization.defaultSerializer <- serializer

    static do System.Threading.ThreadPool.SetMinThreads(100, 100) |> ignore
    static do TcpListenerPool.RegisterListener(IPEndPoint.any)
    static let endPoint = 
        let listener = TcpListenerPool.GetListeners(IPEndPoint.any) |> Seq.head
        listener.LocalEndPoint

    static member Publish(actor : Actor<'T>) =
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.btcp() ] 
        |> Actor.start
        |> Actor.ref

    static member LocalEndPoint = endPoint

//
//  Distributed latch implementation
//

type private LatchMessage =
    | Increment of IReplyChannel<int>
    | GetValue of IReplyChannel<int>

type Latch private (source : ActorRef<LatchMessage>) =
    member __.Increment() = source <!= Increment
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
        source <!= fun ch -> SetResult(index, value, ch)

    member __.IsCompleted = source <!= IsCompleted

    member __.ToArray () = source <!= ToArray

    static member Init(size : int) =
        let behaviour ((results : 'T [], count) as state) msg = async {
            match msg with
            | SetResult(idx, value, ch) -> 
                // should check if idx has been already assigned...
                results.[idx] <- value
                do! ch.Reply (count + 1 = size)
                return (results, count + 1)
            | IsCompleted rc ->
                do! rc.Reply ((count = size))
                return state
            | ToArray rc ->
                do! rc.Reply results
                return state
        }

        let buf = Array.zeroCreate<'T> size

        let ref =
            Behavior.stateful (buf,0) behaviour
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
    member c.SetResult result = try source <!= fun ch -> SetResult(result, ch) with _ -> false
    member c.TryGetResult () = try source <!= TryGetResult with _ -> None
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

type DistributedCancellationTokenSource = 
    internal {
        Id : CancellationTokenId
        Source : ActorRef<CancellationTokenManagerMsg>
    }
with
    member ct.Cancel () = ct.Source <-- Cancel ct.Id
    member ct.IsCancellationRequested = ct.Source <!= fun ch -> IsCancellationRequested(ct.Id, ch)
    member ct.GetLocalCancellationToken() =
        let cts = new System.Threading.CancellationTokenSource()

        let rec checkCancellation () = async {
            let! isCancelled = ct.Source <!- fun ch -> IsCancellationRequested(ct.Id, ch)
            if isCancelled then
                cts.Cancel()
                return ()
            else
                do! Async.Sleep 50
                return! checkCancellation ()
        }

        do Async.Start(checkCancellation())

        cts.Token

type CancellationTokenManager private (source : ActorRef<CancellationTokenManagerMsg>) =
    member __.RequestCancellationTokenSource(?parent : DistributedCancellationTokenSource) =
        let ids = parent |> Option.map (fun p -> p.Id)
        let newId = source <!= fun ch -> RequestCancellationTokenSource(ids, ch)
        { Id = newId ; Source = source }

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

type Queue<'T> private (source : ActorRef<QueueMsg<'T>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue t
    member __.TryDequeue () = source <!= TryDequeue

    static member Init() =
        let queue = System.Collections.Generic.Queue<'T> ()
        let behaviour msg = async {
            match msg with
            | EnQueue t -> queue.Enqueue t
            | TryDequeue rc when queue.Count = 0 -> do! rc.Reply None
            | TryDequeue rc ->
                let t = queue.Dequeue()
                do! rc.Reply (Some t)
        }

        let ref =
            Behavior.stateless behaviour
            |> Actor.bind
            |> Actor.Publish

        new Queue<'T>(ref)


//
//  Distributed Resource factory
//

type private ResourceFactoryMsg =
    | RequestResource of ctor:(unit -> obj) * IReplyChannel<obj>

type ResourceFactory private (source : ActorRef<ResourceFactoryMsg>) =
    member __.RequestResource<'T>(factory : unit -> 'T) =
        let ctor () = factory () :> obj
        let res = source <!= fun ch -> RequestResource(ctor, ch)
        res :?> 'T

    member __.RequestLatch(count) = __.RequestResource(fun () -> Latch.Init(count))
    member __.RequestResultAggregator<'T>(count : int) = __.RequestResource(fun () -> ResultAggregator<'T>.Init(count))
    member __.RequestResultCell<'T>() = __.RequestResource(fun () -> ResultCell<'T>.Init())

    static member Init () =
        let behavior (RequestResource(ctor,rc)) = async {
            let r = ctor ()
            do! rc.Reply r
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