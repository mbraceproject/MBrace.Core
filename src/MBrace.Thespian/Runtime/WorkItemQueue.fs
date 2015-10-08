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
type internal PickledWorkItem =
    {
        /// Parent process entry as recorded in cluster.
        Process : ICloudProcessEntry
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
type internal WorkItemLeaseMonitorMsg =
    /// Declare work item completed
    | Completed
    /// Declare work item execution resulted in fault
    | WorkerDeclaredFault of ExceptionDispatchInfo
    /// Declare worker executing work item has died
    | WorkerDeath

type private WorkItemQueueMsg =
    | Enqueue of PickledWorkItem
    | PutBackFaulted of PickledWorkItem * CloudWorkItemFaultInfo
    | BatchEnqueue of PickledWorkItem []
    | TryDequeue of IWorkerId * IReplyChannel<(PickledWorkItem * CloudWorkItemFaultInfo * ActorRef<WorkItemLeaseMonitorMsg>) option>

module private WorkItemLeaseMonitor =
    
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
                (faultInfo : CloudWorkItemFaultInfo) (workItem : PickledWorkItem) 
                (interval : TimeSpan) (worker : IWorkerId) =

        let cts = new CancellationTokenSource()

        // no tail recursive loop, expected to receive single message
        let behaviour (self : Actor<WorkItemLeaseMonitorMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | Completed -> return ()
            | WorkerDeclaredFault edi ->
                let faultCount = faultInfo.FaultCount + 1
                let faultInfo = FaultDeclaredByWorker(faultCount, edi, worker)
                do! queue.AsyncPost(PutBackFaulted (workItem, faultInfo))

            | WorkerDeath ->
                let faultCount = faultInfo.FaultCount + 1
                let faultInfo = WorkerDeathWhileProcessingWorkItem(faultCount, worker)
                do! queue.AsyncPost(PutBackFaulted (workItem, faultInfo))

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
[<Sealed; AutoSerializable(true)>]
type WorkItemLeaseToken internal (pWorkItem : PickledWorkItem, stateF : LocalStateFactory, faultInfo : CloudWorkItemFaultInfo, leaseMonitor : ActorRef<WorkItemLeaseMonitorMsg>) =

    interface ICloudWorkItemLeaseToken with
        member x.Id: CloudWorkItemId = pWorkItem.WorkItemId

        member x.Type = pWorkItem.Type

        member x.WorkItemType = pWorkItem.WorkItemType

        member x.TargetWorker = pWorkItem.Target

        member x.Size = pWorkItem.Pickle.Size

        member x.Process = pWorkItem.Process
        
        member x.FaultInfo = faultInfo
        
        member x.GetWorkItem(): Async<CloudWorkItem> = async {
            let state = stateF.Value
            match pWorkItem.Pickle with
            | Single pj -> return! state.ReadResult pj
            | Batch(i,pjs) -> 
                let! workItems = state.ReadResult pjs
                return workItems.[i]
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
        LastCleanup : DateTimeOffset
    }
with
    static member Empty = { Queue = WorkItemQueueTopic.Empty ; LastCleanup = DateTimeOffset.Now }

and private WorkItemQueueTopic = TopicQueue<IWorkerId, PickledWorkItem * CloudWorkItemFaultInfo>

/// Provides a distributed, fault-tolerant queue implementation
[<AutoSerializable(true)>]
type WorkItemQueue private (source : ActorRef<WorkItemQueueMsg>, localStateF : LocalStateFactory) =

    interface ICloudWorkItemQueue with
        member x.BatchEnqueue(workItems: CloudWorkItem []) = async {
            let localState = localStateF.Value
            if workItems.Length = 0 then return () else
            let id = sprintf "workitems-%O" <| workItems.[0].Id
            // never create new sifts on parallel workflows (batch enqueues)
            let! pickle = localState.CreateResult(workItems, allowNewSifts = false, fileName = id)
            let mkPickle (index:int) (workItem: CloudWorkItem) =
                {
                    Process = workItem.Process
                    WorkItemId = workItem.Id
                    Type = Type.prettyPrintUntyped workItem.Type
                    Target = workItem.TargetWorker
                    WorkItemType = workItem.WorkItemType
                    Pickle = Batch(index, pickle)
                }

            let items = workItems |> Array.mapi mkPickle
            do! source.AsyncPost (BatchEnqueue items)
        }
        
        member x.Enqueue (workItem: CloudWorkItem, isClientEnqueue:bool) = async {
            let localState = localStateF.Value
            let id = sprintf "workitem-%O" workItem.Id
            // only sift new large objects on client side enqueues
            let! pickle = localState.CreateResult(workItem, allowNewSifts = isClientEnqueue, fileName = id)
            let item =
                {
                    WorkItemId = workItem.Id
                    Process = workItem.Process
                    WorkItemType = workItem.WorkItemType
                    Type = Type.prettyPrintUntyped workItem.Type
                    Target = workItem.TargetWorker
                    Pickle = Single pickle
                }

            do! source.AsyncPost (Enqueue item)
        }
        
        member x.TryDequeue(worker : IWorkerId) = async {
            let! result = source <!- fun ch -> TryDequeue(worker, ch)
            match result with
            | Some(msg, faultState, leaseMonitor) ->
                let leaseToken = new WorkItemLeaseToken(msg, localStateF, faultState, leaseMonitor)
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
            | Enqueue pWorkItem -> 
                localState.Logger.Logf LogLevel.Debug "Enqueued work item of type '%s' and size %s." pWorkItem.Type <| getHumanReadableByteSize pWorkItem.Pickle.Size
                let queue' = state.Queue.Enqueue((pWorkItem, NoFault), ?topic = pWorkItem.Target)
                return { state with Queue = queue' }

            | BatchEnqueue(pWorkItems) ->
                let first = pWorkItems.[0]
                localState.Logger.Logf LogLevel.Debug "Enqueued %d work items of type '%s' and size %s." pWorkItems.Length first.Type <| getHumanReadableByteSize first.Pickle.Size
                let queue' = (state.Queue, pWorkItems) ||> Array.fold (fun q j -> q.Enqueue((j, NoFault), ?topic = j.Target))
                return { state with Queue = queue' }

            | PutBackFaulted(pWorkItem, faultState) ->
                localState.Logger.Logf LogLevel.Debug "Re-enqueued faulted work item %O of type '%s'." pWorkItem.WorkItemId pWorkItem.Type
                let queue' =
                    match faultState with
                    | WorkerDeathWhileProcessingWorkItem _ -> state.Queue.Enqueue((pWorkItem, faultState), ?topic = None)
                    | _ -> state.Queue.Enqueue((pWorkItem, faultState), ?topic = pWorkItem.Target)

                return { state with Queue = queue' }

            | TryDequeue(worker, rc) ->
                let state =
                    if DateTimeOffset.Now - state.LastCleanup > cleanupThreshold then
                        // remove work items from worker topics if inactive.
                        let removed, state' = state.Queue.Cleanup (workerMonitor.IsAlive >> Async.RunSync >> not)
                        let appendRemoved (s:WorkItemQueueTopic) (j : PickledWorkItem, faultState : CloudWorkItemFaultInfo) =
                            let worker = Option.get j.Target
                            localState.Logger.Logf LogLevel.Warning "Redirecting work item %O of type '%s' that has been targeted to dead worker '%s'." j.WorkItemId j.Type worker.Id
                            let j = { j with Target = None }
                            let faultCount = faultState.FaultCount + 1
                            let faultState = IsTargetedWorkItemOfDeadWorker(faultCount, worker)
                            s.Enqueue((j, faultState), ?topic = None)

                        let queue2 = removed |> Seq.fold appendRemoved state'
                        { Queue = queue2 ; LastCleanup = DateTimeOffset.Now }
                    else
                        state

                let! isDeclaredAlive = workerMonitor.IsAlive worker
                match state.Queue.TryDequeue worker with
                | Some((pj,fs), queue') when isDeclaredAlive ->
                    localState.Logger.Logf LogLevel.Debug "Dequeueing work item %O of type '%s'." pj.WorkItemId pj.Type
                    let jlm = WorkItemLeaseMonitor.create workerMonitor self.Ref fs pj (TimeSpan.FromSeconds 1.) worker
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