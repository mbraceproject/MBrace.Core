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
type WorkerId private (processId : ProcessId, address : string) =
    member __.ProcessId = processId
    member __.Id = sprintf "mbrace://%s" address
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

    static member LocalInstance = new WorkerId(ProcessId.LocalInstance, Config.LocalAddress)
         

type private HeartbeatMonitorMsg = 
    | SendHeartbeat
    | CheckHeartbeat
    | GetLatestHeartbeat of IReplyChannel<DateTime>
    | Stop of IReplyChannel<unit>

and private WorkerMonitorMsg =
    | Subscribe of WorkerId * WorkerInfo * IReplyChannel<ActorRef<HeartbeatMonitorMsg> * TimeSpan>
    | UnSubscribe of WorkerId
    | IncrementJobCountBy of WorkerId * int
    | DeclareStatus of WorkerId * WorkerJobExecutionStatus
    | DeclarePerformanceMetrics of WorkerId * PerformanceInfo
    | DeclareDead of WorkerId
    | IsAlive of WorkerId * IReplyChannel<bool>
    | GetAvailableWorkers of IReplyChannel<WorkerState []>
    | TryGetWorkerState of WorkerId * IReplyChannel<WorkerState option>

module private HeartbeatMonitor =
    let create (threshold : TimeSpan) (wmon : ActorRef<WorkerMonitorMsg>) (worker : WorkerId) =
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

    member __.UnSubscribe(worker : IWorkerId) = async {
        return! source.AsyncPost <| UnSubscribe (unbox worker)
    }

    member __.IsAlive(worker : IWorkerId) = async {
        return! source <!- fun ch -> IsAlive(unbox worker, ch)
    }

    interface IWorkerManager with
        member x.DeclareWorkerStatus(id: IWorkerId, status: WorkerJobExecutionStatus): Async<unit> = async {
            return! source.AsyncPost <| DeclareStatus(unbox id, status)
        }

        member x.IncrementJobCount(id: IWorkerId): Async<unit> = async {
            return! source.AsyncPost <| IncrementJobCountBy(unbox id, 1)
        }
        
        member x.DecrementJobCount(id: IWorkerId): Async<unit> = async {
            return! source.AsyncPost <| IncrementJobCountBy(unbox id, -1)
        }
        
        member x.GetAvailableWorkers(): Async<WorkerState []> = async {
            return! source <!- GetAvailableWorkers
        }
        
        member x.SubmitPerformanceMetrics(id: IWorkerId, perf: PerformanceInfo): Async<unit> = async {
            return! source.AsyncPost <| DeclarePerformanceMetrics(unbox id, perf)
        }
        
        member x.SubscribeWorker(id: IWorkerId, info: WorkerInfo): Async<IDisposable> = async {
            let! heartbeatMon, threshold = source <!- fun ch -> Subscribe(unbox id, info, ch)
            let! unsubscriber = HeartbeatMonitor.initHeartbeat threshold heartbeatMon
            return new WorkerSubscriptionManager(x, id, unsubscriber) :> IDisposable
        }
        
        member x.TryGetWorkerState(id: IWorkerId): Async<WorkerState option> = async {
            return! source <!- fun ch -> TryGetWorkerState(unbox id, ch)
        }

    static member Init(?heartbeatThreshold : TimeSpan) =
        let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 4.)
        let behaviour (self : Actor<WorkerMonitorMsg>) (state : Map<WorkerId, WorkerState * ActorRef<HeartbeatMonitorMsg>>) (msg : WorkerMonitorMsg) = async {
            match msg with
            | Subscribe(w, info, rc) ->
                let workerState = 
                    { 
                        Id = unbox w
                        Info = info
                        CurrentJobCount = 0
                        LastHeartbeat = DateTime.Now
                        HeartbeatRate = heartbeatThreshold
                        InitializationTime = DateTime.Now
                        ExecutionStatus = Stopped
                        PerformanceMetrics = PerformanceInfo.Empty
                    }

                let hmon = HeartbeatMonitor.create heartbeatThreshold self.Ref w
                do! rc.Reply(hmon, heartbeatThreshold)
                return state.Add(w, (workerState, hmon))

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

            | DeclareStatus (w,s) ->
                match state.TryFind w with
                | None -> return state
                | Some(info,hm) ->
                    return state.Add(w, ({ info with ExecutionStatus = s}, hm))

            | IncrementJobCountBy (w,i) ->
                match state.TryFind w with
                | None -> return state
                | Some(info,hm) ->
                    return state.Add(w, ({info with CurrentJobCount = info.CurrentJobCount + i}, hm))

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