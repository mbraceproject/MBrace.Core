namespace MBrace.Thespian.Runtime

open System
open System.Threading

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PerformanceMonitor

/// Defines a unique idenfier for an MBrace.Thespian worker
[<AutoSerializable(true)>]
type WorkerId private (processId : ProcessId, uri : string) =
    member __.ProcessId = processId
    member __.Id = uri

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

    /// Gets the worker id instance corresponding to the local worker
    static member LocalInstance = new WorkerId(ProcessId.LocalInstance, Config.LocalMBraceUri)
  
/// Messages sent for actor tasked to monitoring worker heart beats       
type private HeartbeatMonitorMsg = 
    | SendHeartbeat
    | CheckHeartbeat
    | GetLatestHeartbeat of IReplyChannel<DateTime>
    | Stop of IReplyChannel<unit>

/// Global worker monitor actor
and private WorkerMonitorMsg =
    | Subscribe of WorkerId * WorkerInfo * IReplyChannel<ActorRef<HeartbeatMonitorMsg> * TimeSpan>
    | UnSubscribe of WorkerId
    | IncrementWorkItemCountBy of WorkerId * int
    | DeclareStatus of WorkerId * CloudWorkItemExecutionStatus
    | DeclarePerformanceMetrics of WorkerId * PerformanceInfo
    | DeclareDead of WorkerId
    | IsAlive of WorkerId * IReplyChannel<bool>
    | GetAvailableWorkers of IReplyChannel<WorkerState []>
    | TryGetWorkerState of WorkerId * IReplyChannel<WorkerState option>

[<AutoOpen>]
module private HeartbeatMonitor =
    
    /// <summary>
    ///     Creates an actor instance tasked to monitoring heartbeats for supplied worker id.
    /// </summary>
    /// <param name="threshold">Threshold after which worker is to be declared dead.</param>
    /// <param name="workerMonitor">Parent worker monitor that has spawned the heartbeat process.</param>
    /// <param name="worker">Worker to monitor.</param>
    let create (threshold : TimeSpan) (workerMonitor : ActorRef<WorkerMonitorMsg>) (worker : WorkerId) =
        let cts = new CancellationTokenSource()
        let behaviour (self : Actor<HeartbeatMonitorMsg>) (lastRenew : DateTime) (msg : HeartbeatMonitorMsg) = async {
            match msg with
            | SendHeartbeat -> 
                // update latest heartbeat to now
                return DateTime.Now
            | CheckHeartbeat ->
                if DateTime.Now - lastRenew > threshold then
                    // send message to parent worker monitor declaring worker dead
                    workerMonitor <-- DeclareDead worker

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

        // create a polling loop that occasionally checks for heartbeats
        let rec poll () = async {
            aref <-- CheckHeartbeat
            do! Async.Sleep (int threshold.TotalMilliseconds / 5)
            return! poll()
        }

        Async.Start(poll(), cts.Token)
        aref

    /// <summary>
    ///     Initializes a heartbeat loop on the worker side.
    /// </summary>
    /// <param name="threshold">Worker death threshold.</param>
    /// <param name="target">Target heartbeat monitor.</param>
    let initHeartbeat (interval : TimeSpan) (target : ActorRef<HeartbeatMonitorMsg>) = async {
        let cts = new CancellationTokenSource()
        let rec loop () = async {
            try target <-- SendHeartbeat with _ -> ()
            do! Async.Sleep(int interval.TotalMilliseconds)
            return! loop ()
        }

        Async.Start(loop(), cts.Token)
        return { new IDisposable with member __.Dispose() = cts.Cancel() }
    }


/// WorkerManager actor reference used for handling MBrace.Thespian worker instances
[<AutoSerializable(true)>]
type WorkerManager private (heartbeatInterval : TimeSpan, source : ActorRef<WorkerMonitorMsg>) =

    /// <summary>
    ///     Revokes worker subscription for given id.
    /// </summary>
    /// <param name="worker">Worker id to be unscubscribed.</param>
    member __.UnSubscribe(worker : IWorkerId) = async {
        return! source.AsyncPost <| UnSubscribe (unbox worker)
    }

    /// <summary>
    ///     Checks if worker id is still sending heartbeats.
    /// </summary>
    /// <param name="worker">Worker id to be checked.</param>
    member __.IsAlive(worker : IWorkerId) = async {
        return! source <!- fun ch -> IsAlive(unbox worker, ch)
    }

    interface IWorkerManager with
        member x.DeclareWorkerStatus(id: IWorkerId, status: CloudWorkItemExecutionStatus): Async<unit> = async {
            return! source.AsyncPost <| DeclareStatus(unbox id, status)
        }

        member x.IncrementWorkItemCount(id: IWorkerId): Async<unit> = async {
            return! source.AsyncPost <| IncrementWorkItemCountBy(unbox id, 1)
        }
        
        member x.DecrementWorkItemCount(id: IWorkerId): Async<unit> = async {
            return! source.AsyncPost <| IncrementWorkItemCountBy(unbox id, -1)
        }
        
        member x.GetAvailableWorkers(): Async<WorkerState []> = async {
            return! source <!- GetAvailableWorkers
        }
        
        member x.SubmitPerformanceMetrics(id: IWorkerId, perf: PerformanceInfo): Async<unit> = async {
            return! source.AsyncPost <| DeclarePerformanceMetrics(unbox id, perf)
        }
        
        member x.SubscribeWorker(id: IWorkerId, info: WorkerInfo): Async<IDisposable> = async {
            let! heartbeatMon, threshold = source <!- fun ch -> Subscribe(unbox id, info, ch)
            let! unsubscriber = HeartbeatMonitor.initHeartbeat heartbeatInterval heartbeatMon
            return new WorkerSubscriptionManager(x, id, unsubscriber) :> IDisposable
        }
        
        member x.TryGetWorkerState(id: IWorkerId): Async<WorkerState option> = async {
            return! source <!- fun ch -> TryGetWorkerState(unbox id, ch)
        }

    /// <summary>
    ///     Creates a new worker manager instance running in the local process.
    /// </summary>
    /// <param name="heartbeatThreshold">Maximum heartbeat threshold demanded by monitor. Defaults to 10 seconds.</param>
    static member Create(localStateF : LocalStateFactory, ?heartbeatThreshold : TimeSpan, ?heartbeatInterval : TimeSpan) =
        let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 10.)
        let heartbeatInterval = defaultArg heartbeatInterval (TimeSpan.FromSeconds 0.5)
        let logger = localStateF.Value.Logger
        let behaviour (self : Actor<WorkerMonitorMsg>) (state : Map<WorkerId, WorkerState * ActorRef<HeartbeatMonitorMsg>>) (msg : WorkerMonitorMsg) = async {
            match msg with
            | Subscribe(w, info, rc) ->
                let workerState = 
                    { 
                        Id = unbox w
                        Info = info
                        CurrentWorkItemCount = 0
                        LastHeartbeat = DateTime.Now
                        HeartbeatRate = heartbeatThreshold
                        InitializationTime = DateTime.Now
                        ExecutionStatus = Stopped
                        PerformanceMetrics = PerformanceInfo.Empty
                    }

                let hmon = HeartbeatMonitor.create heartbeatThreshold self.Ref w
                do! rc.Reply(hmon, heartbeatThreshold)
                logger.Logf LogLevel.Info "Subscribed new worker '%s' to cluster." w.Id
                return state.Add(w, (workerState, hmon))

            | UnSubscribe w ->
                match state.TryFind w with
                | None -> return state
                | Some (_,hmon) ->
                    do! hmon <!- Stop
                    logger.Logf LogLevel.Info "Unsubscribed worker '%s' from cluster." w.Id
                    return state.Remove w
            
            | DeclareDead w ->
                match state.TryFind w with
                | None -> return state
                | Some (_,hmon) ->
                    do! hmon <!- Stop
                    logger.Logf LogLevel.Error "Worker '%s' has stopped sending heartbeats, declaring dead." w.Id
                    return state.Remove w

            | DeclareStatus (w,s) ->
                match state.TryFind w with
                | None -> return state
                | Some(info,hm) ->
                    match s : CloudWorkItemExecutionStatus with
                    | Stopped -> logger.Logf LogLevel.Info "Worker '%s' has declared itself stopped." w.Id
                    | QueueFault _ -> logger.Logf LogLevel.Warning "Worker '%s' has declared itself in errored state." w.Id
                    | _ -> ()

                    return state.Add(w, ({ info with ExecutionStatus = s}, hm))

            | IncrementWorkItemCountBy (w,i) ->
                match state.TryFind w with
                | None -> return state
                | Some(info,hm) ->
                    return state.Add(w, ({info with CurrentWorkItemCount = info.CurrentWorkItemCount + i}, hm))

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

            | TryGetWorkerState (w, rc) ->
                match state.TryFind w with
                | None -> do! rc.Reply None
                | Some (ws,hmon) ->
                    let! latestHb = hmon <!- GetLatestHeartbeat
                    let ws' = { ws with LastHeartbeat = latestHb }
                    do! rc.Reply (Some ws')

                return state
        }

        let aref =
            Actor.SelfStateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new WorkerManager(heartbeatInterval, aref)


/// Subscribption disposer object
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