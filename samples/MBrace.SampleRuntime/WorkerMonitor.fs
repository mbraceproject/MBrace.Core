namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type WorkerRef private (hostname : string, pid : int) =
    static let localWorker = lazy(
        let hostname = System.Net.Dns.GetHostName()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id
        new WorkerRef(hostname, pid))

    let id = sprintf "mbrace-worker://%s/pid:%d" hostname pid
    member __.Pid = pid
    interface IWorkerRef with
        member __.Hostname = hostname
        member __.Id = id
        member __.Type = "sample runtime worker node"
        // this assumes that workers are constrained to local machine
        member __.ProcessorCount = System.Environment.ProcessorCount
        member __.CompareTo(other : obj) =
            match other with
            | :? WorkerRef as w -> compare id (w :> IWorkerRef).Id
            | _ -> invalidArg "other" "invalid comparand."

    override __.ToString() = id
    override __.Equals other = 
        match other with
        | :? WorkerRef as w -> id = (w :> IWorkerRef).Id
        | _ -> false

    override __.GetHashCode() = hash id

    static member LocalWorker = localWorker.Value

type private HeartbeatMonitorMsg = 
    | SendHeartbeat
    | CheckHeartbeat
    | Stop of IReplyChannel<unit>

and private WorkerMonitorMsg =
    | Subscribe of IWorkerRef * WorkerState * IReplyChannel<ActorRef<HeartbeatMonitorMsg> * TimeSpan>
    | UnSubscribe of IWorkerRef
    | DeclareState of IWorkerRef * WorkerState
    | DeclareDead of IWorkerRef
    | IsAlive of IWorkerRef * IReplyChannel<bool>
    | GetAllWorkers of IReplyChannel<(IWorkerRef * WorkerState) []>

type private WorkerInfo =
    {
        State : WorkerState
        HeartbeatMonitor : ActorRef<HeartbeatMonitorMsg>
    }

module private HeartbeatMonitor =
    let create (threshold : TimeSpan) (wmon : ActorRef<WorkerMonitorMsg>) (worker : IWorkerRef) =
        let cts = new CancellationTokenSource()
        let rec behaviour (lastRenew : DateTime) (self : Actor<HeartbeatMonitorMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | SendHeartbeat -> return! behaviour DateTime.Now self
            | CheckHeartbeat ->
                if DateTime.Now - lastRenew > threshold then
                    wmon <-- DeclareDead worker
                return! behaviour lastRenew self
            | Stop ch -> 
                cts.Cancel()
                do! ch.Reply (())
                return ()
        }

        let aref =
            Actor.bind (behaviour DateTime.Now)
            |> Actor.Publish
            |> Actor.ref

        let rec poll () = async {
            aref <-- SendHeartbeat
            do! Async.Sleep (5 * int threshold.TotalMilliseconds)
        }

        Async.Start(poll(), cts.Token)
        aref

    let initHeartbeat (threshold : TimeSpan) (target : ActorRef<HeartbeatMonitorMsg>) = async {
        let cts = new CancellationTokenSource()
        let rec loop () = async {
            try target <-- SendHeartbeat with _ -> ()
            do! Async.Sleep(int threshold.TotalMilliseconds / 4)
            return! loop ()
        }

        Async.Start(loop(), cts.Token)
        return { new IDisposable with member __.Dispose() = cts.Cancel() }
    }


[<AutoSerializable(true)>]
type WorkerMonitor private (source : ActorRef<WorkerMonitorMsg>) =
    member __.Subscribe(worker : IWorkerRef, initial : WorkerState) = async {
        let! heartbeatMon,threshold = source <!- fun ch -> Subscribe(worker, initial, ch)
        return! HeartbeatMonitor.initHeartbeat threshold heartbeatMon
    }

    member __.DeclareState(worker : IWorkerRef, state : WorkerState) =
        source <-- DeclareState (worker, state)

//    member __.DeclareFaulted(worker : IWorkerRef, edi : ExceptionDispatchInfo) =
//        source <-- DeclareFaulted (worker, edi)
//
//    member __.DeclareJobCount(worker : IWorkerRef, jobCount : int) =
//        source <-- DeclareJobCount (worker, jobCount)
//
//    member __.DeclareRunning(worker : IWorkerRef) =
//        source <-- DeclareNormal worker
//
    member __.UnSubscribe(worker : IWorkerRef) =
        source <-- UnSubscribe worker

    member __.GetAllWorkers() = async {
        return! source <!- GetAllWorkers
    }

    member __.IsAlive(worker : IWorkerRef) = async {
        return! source <!- fun ch -> IsAlive(worker, ch)
    }

    static member Init(?heartbeatThreshold : TimeSpan) =
        let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 2.)
        let rec behaviour (state : Map<IWorkerRef, WorkerInfo>) (self : Actor<WorkerMonitorMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | Subscribe(w, ws, rc) ->
                let hmon = HeartbeatMonitor.create heartbeatThreshold self.Ref w
                do! rc.Reply(hmon, heartbeatThreshold)
                let info = { State = ws ; HeartbeatMonitor = hmon }
                return! behaviour (state.Add(w, info)) self

            | UnSubscribe w ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> 
                    do! info.HeartbeatMonitor <!- Stop
                    return! behaviour (state.Remove w) self
            
            | DeclareDead w ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some { HeartbeatMonitor = hmon } ->
                    do! hmon <!- Stop
                    return! behaviour (state.Remove w) self

            | DeclareState (w, ws) ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> return! behaviour (state.Add(w, { info with State = ws })) self

            | GetAllWorkers rc ->
                let workers = state |> Seq.map (fun kv -> kv.Key, kv.Value.State) |> Seq.toArray
                do! rc.Reply workers
                return! behaviour state self

            | IsAlive (w,rc) ->
                do! rc.Reply (state.ContainsKey w)
                return! behaviour state self
        }

        let aref =
            Actor.bind (behaviour Map.empty)
            |> Actor.Publish
            |> Actor.ref

        new WorkerMonitor(aref)


[<AutoSerializable(false)>]
type WorkerSubscriptionManager private (wmon : WorkerMonitor, worker : WorkerAgent, heartbeatDisposer : IDisposable, stateChangeDisposer : IDisposable) =

    let mutable isDisposed = false
    let lockObj = obj()

    /// Initializes a subscribption and heartbeat for provider worker agent instance
    static member Init(wmon : WorkerMonitor, worker : WorkerAgent) = async {
        let! heartbeatDisposer = wmon.Subscribe(worker.CurrentWorker, worker.CurrentState)
        let stateChangeDisposer = worker.OnStateChange.Subscribe(fun state -> wmon.DeclareState(worker.CurrentWorker, state))
        return new WorkerSubscriptionManager(wmon, worker, heartbeatDisposer, stateChangeDisposer)
    }

    /// Unsubscribes worker from global worker monitor
    member __.UnSubscribe() =
        lock lockObj (fun () ->
            if isDisposed then () else
            stateChangeDisposer.Dispose()
            heartbeatDisposer.Dispose()
            wmon.UnSubscribe worker.CurrentWorker
            isDisposed <- true)

    interface IDisposable with
        member __.Dispose() = __.UnSubscribe()