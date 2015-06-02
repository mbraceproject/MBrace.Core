namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type ImmutableQueue<'T> private (front : 'T list, back : 'T list) =
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

type private LeaseState =
    | Acquired
    | Released
    | Faulted of ExceptionDispatchInfo

type private LeaseMonitorMsg =
    | SetLeaseState of LeaseState
    | GetLeaseState of IReplyChannel<LeaseState>

/// Distributed lease monitor instance
type LeaseMonitor private (threshold : TimeSpan, source : ActorRef<LeaseMonitorMsg>) =
    /// Declare lease to be released successfuly
    member __.Release () = source <-- SetLeaseState Released
    /// Declare fault during lease
    member __.DeclareFault edi = source <-- SetLeaseState (Faulted edi)
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
            | SetLeaseState _, (Faulted _ | Released) -> return state
            | SetLeaseState ls', Acquired -> return (ls', DateTime.Now)
            | GetLeaseState rc, Acquired when DateTime.Now - lastRenew > threshold ->
                let fault = ExceptionDispatchInfo.Capture (new TimeoutException("Heartbeat has timed out."))
                do! rc.Reply (Faulted fault)
                return (Faulted fault, lastRenew)
            | GetLeaseState rc, ls ->
                do! rc.Reply ls
                return state
        }

        let actor =
            Actor.Stateful (Acquired, DateTime.Now) behavior
            |> Actor.Publish

        let faultEvent = new Event<ExceptionDispatchInfo> ()
        let rec poll () = async {
            let! state = actor.Ref <!- GetLeaseState
            match state with
            | Acquired -> 
                do! Async.Sleep(2 * int threshold.TotalMilliseconds)
                return! poll ()
            | Released -> try actor.Stop() with _ -> ()
            | Faulted edi -> try faultEvent.Trigger edi ; actor.Stop() with _ -> () 
        }

        Async.Start(poll ())

        faultEvent.Publish, new LeaseMonitor(threshold, actor.Ref)

//
//  Distributed, fault-tolerant queue implementation
//

type private QueueMsg<'T, 'DequeueToken> =
    | EnQueue of 'T * (* fault count *) (int * ExceptionDispatchInfo) option
    | EnQueueMultiple of 'T []
    | TryDequeue of 'DequeueToken * IReplyChannel<('T * (* fault count *) (int * ExceptionDispatchInfo) option * LeaseMonitor) option>

/// Provides a distributed, fault-tolerant queue implementation
type Queue<'T, 'DequeueToken> private (source : ActorRef<QueueMsg<'T, 'DequeueToken>>) =
    member __.Enqueue (t : 'T) = source <-- EnQueue (t, None)
    member __.EnqueueMultiple (ts : 'T []) = source <-- EnQueueMultiple ts
    member __.TryDequeue (token) = source <!- fun ch -> TryDequeue(token, ch)

    /// Initializes a new distribued queue instance.
    static member Init(shouldDequeue : 'DequeueToken -> 'T -> bool) =
        let self = ref Unchecked.defaultof<ActorRef<QueueMsg<'T, 'DequeueToken>>>
        let behaviour (queue : ImmutableQueue<'T * (int * ExceptionDispatchInfo) option>) msg = async {
            match msg with
            | EnQueue (t, fault) -> return queue.Enqueue (t, fault)
            | EnQueueMultiple ts -> return ts |> Seq.map (fun t -> (t,None)) |> Seq.toList |> queue.EnqueueMultiple
            | TryDequeue (dt, rc) ->
                match queue.TryDequeue() with
                | Some((t, fault), queue') when (try shouldDequeue dt t with _ -> false) ->
                    let putBack, leaseMonitor = LeaseMonitor.Init (TimeSpan.FromSeconds 5.)
                    do! rc.Reply (Some (t, fault, leaseMonitor))
                    let _ = putBack.Subscribe(fun edi -> 
                                                    let faultCount = match fault with None -> 0 | Some(i,_) -> i
                                                    self.Value <-- EnQueue (t, Some(faultCount + 1, edi)))
                    return queue'

                | _ ->
                    do! rc.Reply None
                    return queue
        }

        self :=
            Actor.Stateful ImmutableQueue<'T * (int * ExceptionDispatchInfo) option>.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new Queue<'T, 'DequeueToken>(self.Value)


type internal Pickle =
    | Single of Pickle<CloudJob>
    | Batch of index:int * Pickle<CloudJob []>

and internal PickledJob =
    {
        JobId : string
        ProcessId : string
        Type : string
        Target : IWorkerRef option
        Pickle : Pickle
        Dependencies : AssemblyId []
        ParentTask : ICloudTaskCompletionSource
    }

type JobLeaseToken internal (pjob : PickledJob, faultState, lease : LeaseMonitor) =
    interface ICloudJobLeaseToken with
        member x.DeclareCompleted() = async {
            lease.Release()
        }
        
        member x.DeclareFaulted(edi: ExceptionDispatchInfo) = async {
            return lease.DeclareFault edi
        }
        
        member x.Dependencies: AssemblyId [] = pjob.Dependencies
        
        member x.FaultState: (int * ExceptionDispatchInfo) option = faultState
        
        member x.GetJob(): Async<CloudJob> = async {
            return
                match pjob.Pickle with
                | Single pj -> Config.Serializer.UnPickleTyped pj
                | Batch(i,pjs) -> let js = Config.Serializer.UnPickleTyped pjs in js.[i]
        }
        
        member x.JobId: string = pjob.JobId

        member x.ParentTask: ICloudTaskCompletionSource = pjob.ParentTask
        
        member x.ProcessId: string = pjob.ProcessId
        

[<AutoSerializable(true)>]
type JobQueue private (queueActor : Queue<PickledJob, IWorkerRef>) =
    static member Init(getWorkers : unit -> IWorkerRef[]) =
        // job dequeue predicate -- checks if job is assigned to particular target
        let shouldDequeue (dequeueingWorker : IWorkerRef) (pt : PickledJob) =
            match pt.Target with
            // job not applicable to specific worker, approve dequeue
            | None -> true
            | Some w ->
                // job applicable to current worker, approve dequeue
                if w = dequeueingWorker then true
                else
                    // worker not applicable to current worker, dequeue if target worker has been disposed
                    getWorkers () |> Array.forall ((<>) dequeueingWorker)

        let queue = Queue<_,_>.Init shouldDequeue
        new JobQueue(queue)

    interface IJobQueue with
        member x.BatchEnqueue(jobs: (CloudJob * IWorkerRef option) []) = async {
            // TODO: sifting & cloud values
            let jjobs = jobs |> Array.map fst
            let pickle = Config.Serializer.PickleTyped jjobs
            let mkPickle (index:int) (target : IWorkerRef option) (job : CloudJob) =
                {
                    ProcessId = job.ProcessId
                    JobId = job.JobId
                    Type = job.Type.ToString()
                    Target = target
                    Dependencies = job.Dependencies
                    Pickle = Batch(index, pickle)
                    ParentTask = job.ParentTask
                }

            let items = jobs |> Array.mapi (fun i (j,w) -> mkPickle i w j)
            queueActor.EnqueueMultiple items
        }
        
        member x.Enqueue(job: CloudJob, target: IWorkerRef option) = async {
            let item =
                {
                    ProcessId = job.ProcessId
                    JobId = job.JobId
                    Type = job.Type.ToString()
                    Target = target
                    Dependencies = job.Dependencies
                    Pickle = Single(Config.Serializer.PickleTyped job)
                    ParentTask = job.ParentTask
                }

            queueActor.Enqueue item
        }
        
        member x.TryDequeue(id: IWorkerRef) = async {
            let! result = queueActor.TryDequeue id
            match result with
            | Some(msg, faultState, lease) ->
                // TODO: must be changed
                let! _ = lease.InitHeartBeat()
                let leaseToken = new JobLeaseToken(msg, faultState, lease)
                return Some(leaseToken :> ICloudJobLeaseToken)
            | None -> return None
        }