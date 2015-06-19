namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PerformanceMonitor

[<AutoSerializable(true)>]
type WorkerId private (processId : ProcessId) =
    member __.ProcessId = processId
    member __.Id = sprintf "mbrace://%s/Pid:%d" processId.MachineId.Hostname processId.ProcessId
    interface IWorkerId with
        member x.CompareTo(obj: obj): int =
            match obj with
            | :? WorkerId as w -> compare processId w.ProcessId
            | _ -> invalidArg "obj" "invalid comparand."
        
        member x.Id: string = x.Id

    override x.ToString() = x.Id
    override x.Equals(other:obj) =
        match other with
        | :? WorkerId as w -> processId = w.ProcessId
        | _ -> false

    override x.GetHashCode() = hash processId

    static member LocalInstance = new WorkerId(ProcessId.LocalInstance)
         

type private HeartbeatMonitorMsg = 
    | SendHeartbeat
    | CheckHeartbeat
    | GetLatestHeartbeat of IReplyChannel<DateTime>
    | Stop of IReplyChannel<unit>

and private WorkerMonitorMsg =
    | Subscribe of IWorkerId * IReplyChannel<ActorRef<HeartbeatMonitorMsg> * TimeSpan>
    | UnSubscribe of IWorkerId
    | DeclareState of IWorkerId * WorkerState
    | DeclarePerformanceMetrics of IWorkerId * NodePerformanceInfo
    | DeclareDead of IWorkerId
    | IsAlive of IWorkerId * IReplyChannel<bool>
    | GetAvailableWorkers of IReplyChannel<WorkerInfo []>
    | TryGetWorkerInfo of IWorkerId * IReplyChannel<WorkerInfo option>

module private HeartbeatMonitor =
    let create (threshold : TimeSpan) (wmon : ActorRef<WorkerMonitorMsg>) (worker : IWorkerId) =
        let cts = new CancellationTokenSource()
        let behaviour (self : Actor<HeartbeatMonitorMsg>) (lastRenew : DateTime) (msg : HeartbeatMonitorMsg) = async {
            match msg with
            | SendHeartbeat -> 
                return DateTime.Now
            | CheckHeartbeat ->
                if DateTime.Now - lastRenew > threshold then
                    wmon <-- DeclareDead worker
                return lastRenew
            | GetLatestHeartbeat rc ->
                do! rc.Reply lastRenew
                return lastRenew
            | Stop ch -> 
                cts.Cancel()
                do! ch.Reply (())
                self.Stop ()
                return lastRenew
        }

        let aref =
            Actor.SelfStateful DateTime.Now behaviour
            |> Actor.Publish
            |> Actor.ref

        let rec poll () = async {
            aref <-- CheckHeartbeat
            do! Async.Sleep (int threshold.TotalMilliseconds / 5)
            return! poll()
        }

        Async.Start(poll(), cts.Token)
        aref

    let initHeartbeat (threshold : TimeSpan) (target : ActorRef<HeartbeatMonitorMsg>) = async {
        let cts = new CancellationTokenSource()
        let rec loop () = async {
            try target <-- SendHeartbeat with _ -> ()
            do! Async.Sleep(int threshold.TotalMilliseconds / 5)
            return! loop ()
        }

        Async.Start(loop(), cts.Token)
        return { new IDisposable with member __.Dispose() = cts.Cancel() }
    }


[<AutoSerializable(true)>]
type WorkerManager private (source : ActorRef<WorkerMonitorMsg>) =

    member __.Subscribe(worker : IWorkerId, initial : WorkerState) = async {
        let! heartbeatMon,threshold = source <!- fun ch -> Subscribe(worker, ch)
        return! HeartbeatMonitor.initHeartbeat threshold heartbeatMon
    }

    member __.DeclareState(worker : IWorkerId, state : WorkerState) = async {
        return! source.AsyncPost <| DeclareState (worker, state)
    }

    member __.UnSubscribe(worker : IWorkerId) = async {
        return! source.AsyncPost <| UnSubscribe worker
    }

    member __.GetAvailableWorkers() = async {
        return! source <!- GetAvailableWorkers
    }

    member __.IsAlive(worker : IWorkerId) = async {
        return! source <!- fun ch -> IsAlive(worker, ch)
    }

    interface IWorkerManager with
        member x.DeclareWorkerState(worker: IWorkerId, state: WorkerState): Async<unit> = 
            x.DeclareState(worker, state)
        
        member x.GetAvailableWorkers() =
            x.GetAvailableWorkers()

        member x.TryGetWorkerInfo(ref : IWorkerId) = async {
            return! source <!- fun ch -> TryGetWorkerInfo(ref, ch)
        }
        
        member x.IsValidTargetWorker(target: IWorkerId): Async<bool> = async {
            match target with
            | :? WorkerId -> return! x.IsAlive(target)
            | _ -> return false
        }
        
        member x.SubmitPerformanceMetrics(worker: IWorkerId, perf: NodePerformanceInfo): Async<unit> = async {
            return! source <!- fun ch -> DeclarePerformanceMetrics(worker, perf)
        }
        
        member x.SubscribeWorker(worker: IWorkerId): Async<IDisposable> = async {
            let! heartbeatMon, threshold = source <!- fun ch -> Subscribe(worker, ch)
            let! unsubscriber = HeartbeatMonitor.initHeartbeat threshold heartbeatMon
            return new WorkerSubscriptionManager(x, worker, unsubscriber) :> IDisposable
        }

    static member Init(?heartbeatThreshold : TimeSpan) =
        let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 4.)
        let behaviour (self : Actor<WorkerMonitorMsg>) (state : Map<IWorkerId, WorkerInfo * ActorRef<HeartbeatMonitorMsg>>) (msg : WorkerMonitorMsg) = async {
            match msg with
            | Subscribe(w, rc) ->
                let info =
                    {
                        State = Unchecked.defaultof<_>
                        PerformanceMetrics = Unchecked.defaultof<_>
                        HeartbeatRate = heartbeatThreshold
                        InitializationTime = DateTime.Now
                        LastHeartbeat = DateTime.Now
                        Id = w
                    }

                let hmon = HeartbeatMonitor.create heartbeatThreshold self.Ref w
                do! rc.Reply(hmon, heartbeatThreshold)
                return state.Add(w, (info, hmon))

            | UnSubscribe w ->
                match state.TryFind w with
                | None -> return state
                | Some (_,hmon) -> 
                    do! hmon <!- Stop
                    return state.Remove w
            
            | DeclareDead w ->
                match state.TryFind w with
                | None -> return state
                | Some (_,hmon) ->
                    do! hmon <!- Stop
                    return state.Remove w

            | DeclareState (w, ws) ->
                match state.TryFind w with
                | None -> return state
                | Some (info, hm) -> return state.Add(w, ({ info with State = ws }, hm))

            | DeclarePerformanceMetrics(w, perf) ->
                match state.TryFind w with
                | None -> return state
                | Some (info, hm) -> 
                    let info' = { info with PerformanceMetrics = perf }
                    return state.Add(w, (info', hm))

            | GetAvailableWorkers rc ->
                let x = Unchecked.defaultof<WorkerInfo>
                let workers = state |> Seq.map (function KeyValue(_,(i,_)) -> i) |> Seq.toArray
                do! rc.Reply workers
                return state

            | IsAlive (w,rc) ->
                do! rc.Reply (state.ContainsKey w)
                return state

            | TryGetWorkerInfo (w, rc) ->
                match state.TryFind w with
                | None -> do! rc.Reply None
                | Some (info,hmon) ->
                    let! latestHb = hmon <!- GetLatestHeartbeat
                    let info' = { info with LastHeartbeat = latestHb }
                    do! rc.Reply (Some info')

                return state
        }

        let aref =
            Actor.SelfStateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new WorkerManager(aref)



and [<AutoSerializable(false)>] private 
    WorkerSubscriptionManager (wmon : WorkerManager, currentWorker : IWorkerId, heartbeatDisposer : IDisposable) =

    let mutable isDisposed = false
    let lockObj = obj()

    /// Unsubscribes worker from global worker monitor
    member __.UnSubscribe() =
        lock lockObj (fun () ->
            if isDisposed then () else
            heartbeatDisposer.Dispose()
            wmon.UnSubscribe currentWorker |> Async.RunSync
            isDisposed <- true)

    interface IDisposable with
        member __.Dispose() = __.UnSubscribe()