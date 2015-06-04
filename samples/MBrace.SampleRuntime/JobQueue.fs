namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type internal Pickle =
    | Single of Pickle<CloudJob>
    | Batch of index:int * Pickle<CloudJob []>

type internal PickledJob =
    {
        JobId : string
        ProcessId : string
        Type : string
        Target : IWorkerRef option
        Pickle : Pickle
        Dependencies : AssemblyId []
        ParentTask : ICloudTaskCompletionSource
    }

type internal JobLeaseMonitorMsg =
    | Completed
    | Faulted of ExceptionDispatchInfo

type private JobQueueMsg =
    | Enqueue of PickledJob * faultState:(int * ExceptionDispatchInfo) option
    | BatchEnqueue of PickledJob []
    | TryDequeue of IWorkerRef * IReplyChannel<(PickledJob * (int * ExceptionDispatchInfo) option * ActorRef<JobLeaseMonitorMsg>) option>

module private JobLeaseMonitor =
    
    let create (wmon : WorkerMonitor) (queue : ActorRef<JobQueueMsg>) 
                (faultState : (int * ExceptionDispatchInfo) option) (job : PickledJob) 
                (interval : TimeSpan) (worker : IWorkerRef) =

        let cts = new CancellationTokenSource()

        // no tail recursive loop, expected to receive single message
        let behaviour (self : Actor<JobLeaseMonitorMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | Completed -> return ()
            | Faulted edi ->
                let faultCount = match faultState with Some(count,_) -> count + 1 | None -> 1
                do! queue.AsyncPost(Enqueue (job, Some(faultCount, edi)))

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
                let e = new FaultException(sprintf "Worker '%O' is unresponsive." worker)
                let edi = ExceptionDispatchInfo.Capture e
                ref <-- Faulted edi
        }

        Async.Start(poller(), cts.Token)
        ref

type JobLeaseToken internal (pjob : PickledJob, faultState : (int * ExceptionDispatchInfo) option, 
                                                leaseMonitor : ActorRef<JobLeaseMonitorMsg>) =

    interface ICloudJobLeaseToken with
        member x.DeclareCompleted() = async {
            leaseMonitor <-- Completed
        }
        
        member x.DeclareFaulted(edi: ExceptionDispatchInfo) = async {
            leaseMonitor <-- Faulted edi
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


type private QueueState = QueueTopic<IWorkerRef, PickledJob * (int * ExceptionDispatchInfo) option>

/// Provides a distributed, fault-tolerant queue implementation
[<AutoSerializable(true)>]
type JobQueue private (source : ActorRef<JobQueueMsg>) =

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
            do! source.AsyncPost (BatchEnqueue items)
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

            do! source.AsyncPost (Enqueue (item, None))
        }
        
        member x.TryDequeue(worker : IWorkerRef) = async {
            let! result = source <!- fun ch -> TryDequeue(worker, ch)
            match result with
            | Some(msg, faultState, leaseMonitor) ->
                let leaseToken = new JobLeaseToken(msg, faultState, leaseMonitor)
                return Some(leaseToken :> ICloudJobLeaseToken)
            | None -> return None
        }

    /// Initializes a new distribued queue instance.
    static member Init(wmon : WorkerMonitor, ?cleanupThreshold : TimeSpan) =
        let cleanupThreshold = defaultArg cleanupThreshold (TimeSpan.FromSeconds 10.)

        let rec behaviour (lastCleanup:DateTime) (state : QueueState) (self : Actor<JobQueueMsg>) = async {
            let! msg = self.Receive()
            match msg with
            | Enqueue (pJob, faultState) -> 
                return! behaviour lastCleanup (state.Enqueue(pJob.Target, (pJob, faultState))) self

            | BatchEnqueue(pJobs) ->
                let state = pJobs |> Array.fold (fun (s:QueueState) j -> s.Enqueue(j.Target,(j, None))) state
                return! behaviour lastCleanup state self

            | TryDequeue(worker, rc) ->
                let state, lastCleanup =
                    if DateTime.Now - lastCleanup > cleanupThreshold then
                        // remove jobs from worker topics if inactive.
                        let removed, state' = state.Cleanup (wmon.IsAlive >> Async.RunSync)
                        let appendRemoved (s:QueueState) (j : PickledJob, faultState : (int * ExceptionDispatchInfo) option) =
                            let j = { j with Target = None }
                            let faultCount = match faultState with Some(fc,_) -> fc + 1 | None -> 1
                            let e = new FaultException(sprintf "Worker '%O'is unresponsive." worker)
                            let edi = ExceptionDispatchInfo.Capture e
                            let msg = j, Some(faultCount, edi)
                            s.Enqueue(None, msg)

                        removed |> Seq.fold appendRemoved state', DateTime.Now
                    else
                        state, lastCleanup

                match state.Dequeue worker with
                | None ->
                    do! rc.Reply None
                    return! behaviour lastCleanup state self

                | Some((pj,fs), state') ->
                    let jlm = JobLeaseMonitor.create wmon self.Ref fs pj (TimeSpan.FromSeconds 1.) worker
                    do! rc.Reply(Some(pj, fs, jlm))
                    return! behaviour lastCleanup state' self
        }

        let ref =
            Actor.bind (behaviour DateTime.Now QueueState.Empty)
            |> Actor.Publish
            |> Actor.ref

        new JobQueue(ref)