namespace MBrace.Thespian.Runtime

open System
open System.Threading
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Components

/// Describes a pickled MBrace work item.
/// Can be used without any need for assembly dependencies being loaded.
type internal Pickle =
    /// Single cloud work item
    | Single of workItem:ResultMessage<CloudWorkItem>

    /// Cloud work item is part of a batch enqueue.
    /// Pickled together for size optimization reasons.
    | Batch of index:int * workItems:ResultMessage<CloudWorkItem []>
    /// Size of pickle in bytes
    member p.Size =
        match p with
        | Single sp -> sp.Size
        | Batch(_,sp) -> sp.Size

/// Pickled MBrace work item entry.
/// Can be used without need for assembly dependencies being loaded.
type internal PickledJob =
    {
        /// Parent task entry as recorded in cluster.
        TaskEntry : ICloudTaskCompletionSource
        /// Unique work item identifier
        WorkItemId : CloudWorkItemId
        /// Work item type enumeration
        WorkItemType : CloudWorkItemType
        /// Work item pretty printed return type
        Type : string
        /// Declared target worker for work item
        Target : IWorkerId option
        /// Pickled work item contents
        Pickle : Pickle
    }

/// Messages accepted by a work item monitoring actor
type internal JobLeaseMonitorMsg =
    /// Declare work item completed
    | Completed
    /// Declare work item execution resulted in fault
    | WorkerDeclaredFault of ExceptionDispatchInfo
    /// Declare worker executing work item has died
    | WorkerDeath

type private WorkItemQueueMsg =
    | Enqueue of PickledJob * CloudWorkItemFaultInfo
    | BatchEnqueue of PickledJob []
    | TryDequeue of IWorkerId * IReplyChannel<(PickledJob * CloudWorkItemFaultInfo * ActorRef<JobLeaseMonitorMsg>) option>

module private JobLeaseMonitor =
    
    /// <summary>
    ///     Creates an local actor that monitors work item execution progress.
    ///     It is tasked to re-enqueue the work item in case of fault/worker death.
    /// </summary>
    /// <param name="workerMonitor">Cluster worker monitor.</param>
    /// <param name="queue">Parent work item queue actor.</param>
    /// <param name="faultInfo">Current fault state of the work item.</param>
    /// <param name="workItem">Work item instance being monitored.</param>
    /// <param name="interval">Monitor interval.</param>
    /// <param name="worker">Executing worker id.</param>
    let create (workerMonitor : WorkerManager) (queue : ActorRef<WorkItemQueueMsg>) 
                (faultInfo : CloudWorkItemFaultInfo) (job : PickledJob) 
                (interval : TimeSpan) (worker : IWorkerId) =

        let cts = new CancellationTokenSource()

        // no tail recursive loop, expected to receive single message
        let behaviour (self : Actor<JobLeaseMonitorMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | Completed -> return ()
            | WorkerDeclaredFault edi ->
                let faultCount = faultInfo.FaultCount + 1
                let faultInfo = FaultDeclaredByWorker(faultCount, edi, worker)
                do! queue.AsyncPost(Enqueue (job, faultInfo))

            | WorkerDeath ->
                let faultCount = faultInfo.FaultCount + 1
                let faultInfo = WorkerDeathWhileProcessingWorkItem(faultCount, worker)
                do! queue.AsyncPost(Enqueue (job, faultInfo))

            cts.Cancel()
        }

        let ref = Actor.bind behaviour |> Actor.Publish |> Actor.ref
        let rec poller () = async {
            let! isAlive = workerMonitor.IsAlive worker |> Async.Catch
            match isAlive with
            | Choice1Of2 true | Choice2Of2 _ ->
                do! Async.Sleep(int interval.TotalMilliseconds)
                return! poller ()

            | Choice1Of2 false ->
                ref <-- WorkerDeath
        }

        Async.Start(poller(), cts.Token)
        ref

/// Work item lease token implementation, received when dequeuing a work item from the queue.
[<AutoSerializable(true)>]
type JobLeaseToken internal (pjob : PickledJob, stateF : LocalStateFactory, faultInfo : CloudWorkItemFaultInfo, leaseMonitor : ActorRef<JobLeaseMonitorMsg>) =

    interface ICloudWorkItemLeaseToken with
        member x.Id: CloudWorkItemId = pjob.WorkItemId

        member x.Type = pjob.Type

        member x.WorkItemType = pjob.WorkItemType

        member x.TargetWorker = pjob.Target

        member x.Size = pjob.Pickle.Size

        member x.TaskEntry = pjob.TaskEntry
        
        member x.FaultInfo = faultInfo
        
        member x.GetWorkItem(): Async<CloudWorkItem> = async {
            let state = stateF.Value
            match pjob.Pickle with
            | Single pj -> return! state.ReadResult pj
            | Batch(i,pjs) -> 
                let! jobs = state.ReadResult pjs
                return jobs.[i]
        }

        member x.DeclareCompleted() = async {
            leaseMonitor <-- Completed
        }
        
        member x.DeclareFaulted(edi: ExceptionDispatchInfo) = async {
            leaseMonitor <-- WorkerDeclaredFault edi
        }
        


/// Work item Queue actor state
type private QueueState = 
    {
        Queue : WorkItemQueueTopic
        LastCleanup : DateTime
    }

    static member Empty = { Queue = WorkItemQueueTopic.Empty ; LastCleanup = DateTime.Now }

and private WorkItemQueueTopic = TopicQueue<IWorkerId, PickledJob * CloudWorkItemFaultInfo>

/// Provides a distributed, fault-tolerant queue implementation
[<AutoSerializable(true)>]
type WorkItemQueue private (source : ActorRef<WorkItemQueueMsg>, localStateF : LocalStateFactory) =

    interface ICloudWorkItemQueue with
        member x.BatchEnqueue(workItems: CloudWorkItem []) = async {
            let localState = localStateF.Value
            if jobs.Length = 0 then return () else
            let id = sprintf "jobs-%O" <| jobs.[0].Id
            // never create new sifts on parallel workflows (batch enqueues)
            let! pickle = localState.CreateResult(jobs, allowNewSifts = false, fileName = id)
            let mkPickle (index:int) (job : CloudWorkItem) =
                {
                    TaskEntry = work item.TaskEntry
                    WorkItemId = work item.Id
                    Type = Type.prettyPrintUntyped work item.Type
                    Target = work item.TargetWorker
                    WorkItemType = work item.WorkItemType
                    Pickle = Batch(index, pickle)
                }

            let items = jobs |> Array.mapi mkPickle
            do! source.AsyncPost (BatchEnqueue items)
        }
        
        member x.Enqueue (workItem: CloudWorkItem, isClientEnqueue:bool) = async {
            let localState = localStateF.Value
            let id = sprintf "job-%O" work item.Id
            // only sift new large objects on client side enqueues
            let! pickle = localState.CreateResult(job, allowNewSifts = isClientEnqueue, fileName = id)
            let item =
                {
                    TaskEntry = work item.TaskEntry
                    WorkItemId = work item.Id
                    WorkItemType = work item.WorkItemType
                    Type = Type.prettyPrintUntyped work item.Type
                    Target = work item.TargetWorker
                    Pickle = Single pickle
                }

            do! source.AsyncPost (Enqueue (item, NoFault))
        }
        
        member x.TryDequeue(worker : IWorkerId) = async {
            let! result = source <!- fun ch -> TryDequeue(worker, ch)
            match result with
            | Some(msg, faultState, leaseMonitor) ->
                let leaseToken = new JobLeaseToken(msg, localStateF, faultState, leaseMonitor)
                return Some(leaseToken :> ICloudWorkItemLeaseToken)
            | None -> return None
        }

    /// <summary>
    ///     Creates a new work item queue instance running in the local process.
    /// </summary>
    /// <param name="workerMonitor">Worker monitor instance.</param>
    /// <param name="localStateF">Local state factory instance.</param>
    /// <param name="cleanupInterval">Topic queue cleanup interval. Defaults to 10 seconds.</param>
    static member Create(workerMonitor : WorkerManager, localStateF : LocalStateFactory, ?cleanupInterval : TimeSpan) =
        let cleanupThreshold = defaultArg cleanupInterval (TimeSpan.FromSeconds 10.)
        let localState = localStateF.Value
        let behaviour (self : Actor<WorkItemQueueMsg>) (state : QueueState) (msg : WorkItemQueueMsg) = async {
            match msg with
            | Enqueue (pJob, faultState) -> 
                localState.Logger.Logf LogLevel.Debug "Enqueued work item of type '%s' and size %s." pJob.Type <| getHumanReadableByteSize pJob.Pickle.Size
                let queue' = state.Queue.Enqueue((pJob, faultState), ?topic = pJob.Target)
                return { state with Queue = queue' }

            | BatchEnqueue(pJobs) ->
                let first = pJobs.[0]
                localState.Logger.Logf LogLevel.Debug "Enqueued %d jobs of type '%s' and size %s." pJobs.Length first.Type <| getHumanReadableByteSize first.Pickle.Size
                let queue' = (state.Queue, pJobs) ||> Array.fold (fun q j -> q.Enqueue((j, NoFault), ?topic = j.Target))
                return { state with Queue = queue' }

            | TryDequeue(worker, rc) ->
                let state =
                    if DateTime.Now - state.LastCleanup > cleanupThreshold then
                        // remove jobs from worker topics if inactive.
                        let removed, state' = state.Queue.Cleanup (workerMonitor.IsAlive >> Async.RunSync >> not)
                        let appendRemoved (s:WorkItemQueueTopic) (j : PickledJob, faultState : CloudWorkItemFaultInfo) =
                            let worker = Option.get j.Target
                            localState.Logger.Logf LogLevel.Warning "Redirecting work item '%O' of type '%s' that has been targeted to dead worker '%s'." j.WorkItemId j.Type worker.Id
                            let j = { j with Target = None }
                            let faultCount = faultState.FaultCount + 1
                            let faultState = IsTargetedWorkItemOfDeadWorker(faultCount, worker)
                            s.Enqueue((j, faultState), ?topic = None)

                        let queue2 = removed |> Seq.fold appendRemoved state'
                        { Queue = queue2 ; LastCleanup = DateTime.Now }
                    else
                        state

                let! isDeclaredAlive = workerMonitor.IsAlive worker
                match state.Queue.TryDequeue worker with
                | Some((pj,fs), queue') when isDeclaredAlive ->
                    localState.Logger.Logf LogLevel.Debug "Dequeueing work item %O of type '%s'." pj.WorkItemId pj.Type
                    let jlm = JobLeaseMonitor.create workerMonitor self.Ref fs pj (TimeSpan.FromSeconds 1.) worker
                    do! rc.Reply(Some(pj, fs, jlm))
                    return { state with Queue = queue' }

                | _ ->
                    do! rc.Reply None
                    return state

        }

        let ref =
            Actor.SelfStateful QueueState.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new WorkItemQueue(ref, localStateF)