namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.PrettyPrinters

type internal Pickle =
    | Single of Pickle<CloudJob>
    | Batch of index:int * Pickle<CloudJob []>
with
    member p.Size =
        match p with
        | Single sp -> sp.Bytes.LongLength
        | Batch(_,sp) -> sp.Bytes.LongLength

type internal PickledJob =
    {
        TaskEntry : ICloudTaskEntry
        JobId : string
        JobType : JobType
        Type : string
        Target : IWorkerId option
        Pickle : Pickle
    }

type internal JobLeaseMonitorMsg =
    | Completed
    | WorkerDeclaredFault of ExceptionDispatchInfo
    | WorkerDeath

type private JobQueueMsg =
    | Enqueue of PickledJob * JobFaultInfo
    | BatchEnqueue of PickledJob []
    | TryDequeue of IWorkerId * IReplyChannel<(PickledJob * JobFaultInfo * ActorRef<JobLeaseMonitorMsg>) option>

module private JobLeaseMonitor =
    
    let create (wmon : WorkerManager) (queue : ActorRef<JobQueueMsg>) 
                (faultInfo : JobFaultInfo) (job : PickledJob) 
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
                let faultInfo = WorkerDeathWhileProcessingJob(faultCount, worker)
                do! queue.AsyncPost(Enqueue (job, faultInfo))

            cts.Cancel()
        }

        let ref = Actor.bind behaviour |> Actor.Publish |> Actor.ref
        let rec poller () = async {
            let! isAlive = wmon.IsAlive worker |> Async.Catch
            match isAlive with
            | Choice1Of2 true | Choice2Of2 _ ->
                do! Async.Sleep(int interval.TotalMilliseconds)
                return! poller ()

            | Choice1Of2 false ->
                ref <-- WorkerDeath
        }

        Async.Start(poller(), cts.Token)
        ref

type JobLeaseToken internal (pjob : PickledJob, faultInfo : JobFaultInfo, leaseMonitor : ActorRef<JobLeaseMonitorMsg>) =

    interface ICloudJobLeaseToken with
        member x.DeclareCompleted() = async {
            leaseMonitor <-- Completed
        }
        
        member x.DeclareFaulted(edi: ExceptionDispatchInfo) = async {
            leaseMonitor <-- WorkerDeclaredFault edi
        }

        member x.Type = pjob.Type

        member x.JobType = pjob.JobType

        member x.TargetWorker = pjob.Target

        member x.Size = pjob.Pickle.Size

        member x.TaskEntry = pjob.TaskEntry
        
        member x.FaultInfo = faultInfo
        
        member x.GetJob(): Async<CloudJob> = async {
            return
                match pjob.Pickle with
                | Single pj -> Config.Serializer.UnPickleTyped pj
                | Batch(i,pjs) -> let js = Config.Serializer.UnPickleTyped pjs in js.[i]
        }
        
        member x.Id: string = pjob.JobId

type private QueueState = 
    {
        Queue : JobQueueTopic
        LastCleanup : DateTime
    }
with
    static member Empty = { Queue = JobQueueTopic.Empty ; LastCleanup = DateTime.Now }

and private JobQueueTopic = QueueTopic<IWorkerId, PickledJob * JobFaultInfo>

/// Provides a distributed, fault-tolerant queue implementation
[<AutoSerializable(true)>]
type JobQueue private (source : ActorRef<JobQueueMsg>) =

    interface IJobQueue with
        member x.BatchEnqueue(jobs: CloudJob []) = async {
            // TODO: sifting & cloud values
            let pickle = Config.Serializer.PickleTyped jobs
            let mkPickle (index:int) (job : CloudJob) =
                {
                    TaskEntry = job.TaskEntry
                    JobId = job.Id
                    Type = Type.prettyPrint job.Type
                    Target = job.TargetWorker
                    JobType = job.JobType
                    Pickle = Batch(index, pickle)
                }

            let items = jobs |> Array.mapi mkPickle
            do! source.AsyncPost (BatchEnqueue items)
        }
        
        member x.Enqueue (job: CloudJob) = async {
            let item =
                {
                    TaskEntry = job.TaskEntry
                    JobId = job.Id
                    JobType = job.JobType
                    Type = Type.prettyPrint job.Type
                    Target = job.TargetWorker
                    Pickle = Single(Config.Serializer.PickleTyped job)
                }

            do! source.AsyncPost (Enqueue (item, NoFault))
        }
        
        member x.TryDequeue(worker : IWorkerId) = async {
            let! result = source <!- fun ch -> TryDequeue(worker, ch)
            match result with
            | Some(msg, faultState, leaseMonitor) ->
                let leaseToken = new JobLeaseToken(msg, faultState, leaseMonitor)
                return Some(leaseToken :> ICloudJobLeaseToken)
            | None -> return None
        }

    /// Initializes a new distribued queue instance.
    static member Init(wmon : WorkerManager, ?cleanupThreshold : TimeSpan) =
        let cleanupThreshold = defaultArg cleanupThreshold (TimeSpan.FromSeconds 10.)

        let behaviour (self : Actor<JobQueueMsg>) (state : QueueState) (msg : JobQueueMsg) = async {
            match msg with
            | Enqueue (pJob, faultState) -> 
                let queue' = state.Queue.Enqueue(pJob.Target, (pJob, faultState))
                return { state with Queue = queue' }

            | BatchEnqueue(pJobs) ->
                let queue' = (state.Queue, pJobs) ||> Array.fold (fun q j -> q.Enqueue(j.Target,(j, NoFault)))
                return { state with Queue = queue' }

            | TryDequeue(worker, rc) ->
                let state =
                    if DateTime.Now - state.LastCleanup > cleanupThreshold then
                        // remove jobs from worker topics if inactive.
                        let removed, state' = state.Queue.Cleanup (wmon.IsAlive >> Async.RunSync >> not)
                        let appendRemoved (s:JobQueueTopic) (j : PickledJob, faultState : JobFaultInfo) =
                            let worker = Option.get j.Target
                            let j = { j with Target = None }
                            let faultCount = faultState.FaultCount + 1
                            let faultState = IsTargetedJobOfDeadWorker(faultCount, worker)
                            s.Enqueue(None, (j, faultState))

                        let queue2 = removed |> Seq.fold appendRemoved state'
                        { Queue = queue2 ; LastCleanup = DateTime.Now }
                    else
                        state

                match state.Queue.Dequeue worker with
                | None ->
                    do! rc.Reply None
                    return state

                | Some((pj,fs), queue') ->
                    let jlm = JobLeaseMonitor.create wmon self.Ref fs pj (TimeSpan.FromSeconds 1.) worker
                    do! rc.Reply(Some(pj, fs, jlm))
                    return { state with Queue = queue' }
        }

        let ref =
            Actor.SelfStateful QueueState.Empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new JobQueue(ref)