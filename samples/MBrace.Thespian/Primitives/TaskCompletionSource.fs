namespace MBrace.Thespian.Runtime

//
// This section defines a runtime entry for a running Cloud task
//

open System

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Store

type private TaskCompletionSourceMsg =
    | GetState of IReplyChannel<CloudTaskState>
    | TrySetResult of PickleOrFile<TaskResult> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<PickleOrFile<TaskResult> option>
    | DeclareStatus of status:CloudTaskStatus
    | IncrementJobCount
    | DeclareCompletedJob
    | DeclareFaultedJob

/// Task completion source execution state
type private TaskCompletionSourceState = 
    {
        /// Persisted result of task, if available
        Result : PickleOrFile<TaskResult> option
        /// Cloud task metadata
        Info : CloudTaskInfo
        /// Task execution status
        Status : CloudTaskStatus
        /// Number of currently executing MBrace jobs for task
        ActiveJobCount : int
        /// Maximum number of concurrently executing MBrace jobs for task
        MaxActiveJobCount : int
        /// Total number of MBrace jobs for task
        TotalJobCount : int
        /// Total number of completed jobs for task
        CompletedJobCount : int
        /// Total number of faulted jobs for task
        FaultedJobCount : int
        /// Task execution time representation
        ExecutionTime: ExecutionTime
    }
with 
    /// <summary>
    ///     Initializes task state using provided task metadata.
    /// </summary>
    /// <param name="info">Task metadata.</param>
    static member Init(info : CloudTaskInfo) = 
        { 
            Info = info ; Result = None
            TotalJobCount = 0 ; ActiveJobCount = 0 ; MaxActiveJobCount = 0 ; 
            CompletedJobCount = 0 ; FaultedJobCount = 0
            ExecutionTime = NotStarted ; Status = Posted 
        }

    /// <summary>
    ///     Converts the internal task representation to one that can by used by
    ///     the MBrace task manager.
    /// </summary>
    /// <param name="ts">Task state.</param>
    static member ExportState(ts : TaskCompletionSourceState) : CloudTaskState =
        {
            Status = ts.Status
            Info = ts.Info
            ActiveJobCount = ts.ActiveJobCount
            TotalJobCount = ts.TotalJobCount
            MaxActiveJobCount = ts.MaxActiveJobCount
            CompletedJobCount = ts.CompletedJobCount
            FaultedJobCount = ts.FaultedJobCount

            ExecutionTime = 
                match ts.ExecutionTime with 
                | Started (t,_) -> Started (t, DateTime.Now - t)
                | et -> et
        }

/// Actor TaskEntry implementation
[<AutoSerializable(true)>]
type ActorTaskCompletionSource private (source : ActorRef<TaskCompletionSourceMsg>, id : string, info : CloudTaskInfo, pvm : PersistedValueManager)  =
    member __.Id = id
    member __.Info = info

    interface ICloudTaskCompletionSource with
        member x.Id = id
        member x.AwaitResult(): Async<TaskResult> = async {
            let rec awaiter () = async {
                let! result = source <!- TryGetResult
                match result with
                | Some r -> return! r.GetValueAsync()
                | None ->
                    do! Async.Sleep 200
                    return! awaiter ()
            }

            return! awaiter()
        }
        
        member x.DeclareCompletedJob(): Async<unit> = async {
            return! source.AsyncPost DeclareCompletedJob
        }
        
        member x.DeclareFaultedJob(): Async<unit> = async {
            return! source.AsyncPost DeclareFaultedJob
        }
        
        member x.DeclareStatus(status: CloudTaskStatus): Async<unit> = async {
            return! source.AsyncPost (DeclareStatus status)
        }
        
        member x.GetState(): Async<CloudTaskState> = async {
            return! source <!- GetState
        }
        
        member x.IncrementJobCount(): Async<unit> =  async {
            return! source.AsyncPost IncrementJobCount
        }
        
        member x.Info: CloudTaskInfo = info
        
        member x.TryGetResult(): Async<TaskResult option> = async {
            let! rp = source <!- TryGetResult
            match rp with
            | None -> return None
            | Some pv ->
                let! r = pv.GetValueAsync()
                return Some r
        }
        
        member x.TrySetResult(result: TaskResult): Async<bool> = async {
            let id = sprintf "taskResult-%s" <| mkUUID()
            let! pv = pvm.CreateFileOrPickleAsync(result, id)
            return! source <!- fun ch -> TrySetResult(pv, ch)
        }

    /// <summary>
    ///     Creates a task entry instance in local process.
    /// </summary>
    /// <param name="id">Task unique identifier.</param>
    /// <param name="info">Task metadata.</param>
    static member Create(id : string, info : CloudTaskInfo, pvm : PersistedValueManager) =
        let behaviour (state : TaskCompletionSourceState) (msg : TaskCompletionSourceMsg) = async {
            match msg with
            | GetState rc ->
                do! rc.Reply (TaskCompletionSourceState.ExportState state)
                return state

            | TrySetResult(r, rc) when Option.isSome state.Result ->
                do! rc.Reply false
                return state

            | TrySetResult(r, rc) ->
                do! rc.Reply true
                return { state with Result = Some r }

            | TryGetResult rc ->
                do! rc.Reply state.Result
                return state

            | DeclareStatus status ->
                let executionTime =
                    match state.ExecutionTime, status with
                    | NotStarted, Running -> Started (DateTime.Now, TimeSpan.Zero)
                    | Started (t,_), (Completed | UserException | Canceled) -> 
                        let now = DateTime.Now
                        Finished(t, now - t, now)
                    | et, _ -> et

                return { state with Status = status ; ExecutionTime = executionTime}

            | IncrementJobCount ->
                return { state with 
                                TotalJobCount = state.TotalJobCount + 1 ; 
                                ActiveJobCount = state.ActiveJobCount + 1 ;
                                MaxActiveJobCount = max state.MaxActiveJobCount (1 + state.ActiveJobCount) }

            | DeclareCompletedJob ->
                return { state with ActiveJobCount = state.ActiveJobCount - 1 ; CompletedJobCount = state.CompletedJobCount + 1 }

            | DeclareFaultedJob ->
                return { state with ActiveJobCount = state.ActiveJobCount - 1 ; FaultedJobCount = state.FaultedJobCount + 1 }
        }

        let ref =
            Behavior.stateful (TaskCompletionSourceState.Init info) behaviour
            |> Actor.bind
            |> Actor.Publish
            |> Actor.ref

        new ActorTaskCompletionSource(ref, id, info, pvm)