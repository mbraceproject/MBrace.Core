namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type WorkerRef private (hostname : string, pid : int) =
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

    static member LocalWorker = 
        let hostname = System.Net.Dns.GetHostName()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id
        new WorkerRef(hostname, pid)

type private HeartbeatMonitorMsg = 
    | SendHeartbeat
    | CheckHeartbeat
    | Stop of IReplyChannel<unit>

and private WorkerMonitorMsg =
    | Subscribe of IWorkerRef * IReplyChannel<ActorRef<HeartbeatMonitorMsg> * TimeSpan>
    | DeclareDead of IWorkerRef
    | DeclareFaulted of IWorkerRef * ExceptionDispatchInfo
    | DeclareWorking of IWorkerRef
    | DeclareJobCount of IWorkerRef * int
    | UnSubscribe of IWorkerRef
    | IsAlive of IWorkerRef * IReplyChannel<bool>
    | GetAllWorkers of IReplyChannel<(ExceptionDispatchInfo option * int * IWorkerRef) []>

type private WorkerInfo =
    {
        Fault : ExceptionDispatchInfo option
        JobCount : int
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
    member __.Subscribe(worker : IWorkerRef) = async {
        let! heartbeatMon,threshold = source <!- fun ch -> Subscribe(worker, ch)
        return! HeartbeatMonitor.initHeartbeat threshold heartbeatMon
    }

    member __.DeclareFaulted(worker : IWorkerRef, edi : ExceptionDispatchInfo) =
        source <-- DeclareFaulted (worker, edi)

    member __.DeclareJobCount(worker : IWorkerRef, jobCount : int) =
        source <-- DeclareJobCount (worker, jobCount)

    member __.DeclareRunning(worker : IWorkerRef) =
        source <-- DeclareWorking worker

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
            | Subscribe(w, rc) ->
                let hmon = HeartbeatMonitor.create heartbeatThreshold self.Ref w
                do! rc.Reply(hmon, heartbeatThreshold)
                let info = { Fault = None ; HeartbeatMonitor = hmon ; JobCount = 0 }
                return! behaviour (state.Add(w, info)) self
            
            | DeclareDead w ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some { HeartbeatMonitor = hmon } ->
                    do! hmon <!- Stop
                    return! behaviour (state.Remove w) self

            | DeclareFaulted (w, edi) ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> return! behaviour (state.Add(w, {info with Fault = Some edi })) self

            | DeclareWorking w ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> return! behaviour (state.Add(w, {info with Fault = None })) self

            | DeclareJobCount (w, j) ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> return! behaviour (state.Add(w, {info with JobCount = j})) self

            | UnSubscribe w ->
                match state.TryFind w with
                | None -> return! behaviour state self
                | Some info -> 
                    do! info.HeartbeatMonitor <!- Stop
                    return! behaviour (state.Remove w) self

            | GetAllWorkers rc ->
                let workers = state |> Seq.map (fun kv -> kv.Value.Fault, kv.Value.JobCount, kv.Key) |> Seq.toArray
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
type WorkerManager private (wmon : WorkerMonitor, currentWorker : IWorkerRef, unsubscriber : IDisposable) =

    let mutable unsubscriber = unsubscriber

    static member Create(wmon : WorkerMonitor) = async {
        let currentWorker = WorkerRef.LocalWorker
        let! unsubscriber = wmon.Subscribe(currentWorker)
        return new WorkerManager(wmon, currentWorker, unsubscriber)
    }

    interface IWorkerManager with
        member x.CurrentWorker: IWorkerRef = currentWorker
        
        member x.DeclareFaulted(edi: ExceptionDispatchInfo) = async {
            return wmon.DeclareFaulted(currentWorker, edi)
        }
        
        member x.DeclareJobCount(jobCount: int): Async<unit> = async {
            return wmon.DeclareJobCount(currentWorker, jobCount)
        }
        
        member x.DeclareRunning(): Async<unit> = async {
            return wmon.DeclareRunning currentWorker
        }

        member x.DeclareStopped(): Async<unit> = async {
            return wmon.UnSubscribe currentWorker
        }