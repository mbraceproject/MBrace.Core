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
    | TrySetResult of ResultMessage<TaskResult> * IWorkerId * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<ResultMessage<TaskResult> option>
    | DeclareStatus of status:CloudTaskStatus
    | IncrementJobCount
    | IncrementCompletedWorkItemCount
    | IncrementFaultedWorkItemCount

/// Task completion source execution state
type private TaskCompletionSourceState = 
    {
        /// Persisted result of task, if available
        Result : ResultMessage<TaskResult> option
        /// Cloud task metadata
        Info : CloudTaskInfo
        /// Task execution status
        Status : CloudTaskStatus
        /// Number of currently executing MBrace jobs for task
        ActiveWorkItemCount : int
        /// Maximum number of concurrently executing MBrace jobs for task
        MaxActiveWorkItemCount : int
        /// Total number of MBrace jobs for task
        TotalWorkItemCount : int
        /// Total number of completed jobs for task
        CompletedWorkItemCount : int
        /// Total number of faulted jobs for task
        FaultedWorkItemCount : int
        /// Task execution time representation
        ExecutionTime: ExecutionTime
    }

    /// <summary>
    ///     Initializes task state using provided task metadata.
    /// </summary>
    /// <param name="info">Task metadata.</param>
    static member Init(info : CloudTaskInfo) = 
        { 
            Info = info ; Result = None
            TotalWorkItemCount = 0 ; ActiveWorkItemCount = 0 ; MaxActiveWorkItemCount = 0 ; 
            CompletedWorkItemCount = 0 ; FaultedWorkItemCount = 0
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
            ActiveWorkItemCount = ts.ActiveWorkItemCount
            TotalWorkItemCount = ts.TotalWorkItemCount
            MaxActiveWorkItemCount = ts.MaxActiveWorkItemCount
            CompletedWorkItemCount = ts.CompletedWorkItemCount
            FaultedWorkItemCount = ts.FaultedWorkItemCount

            ExecutionTime = 
                match ts.ExecutionTime with 
                | Started (t,_) -> Started (t, DateTime.Now - t)
                | et -> et
        }

/// Actor TaskEntry implementation
[<AutoSerializable(true)>]
type ActorTaskCompletionSource private (localStateF : LocalStateFactory, source : ActorRef<TaskCompletionSourceMsg>, id : string, info : CloudTaskInfo)  =
    member __.Id = id
    member __.Info = info

    interface ICloudTaskCompletionSource with
        member x.Id = id
        member x.AwaitResult(): Async<TaskResult> = async {
            let localState = localStateF.Value
            let rec awaiter () = async {
                let! result = source <!- TryGetResult
                match result with
                | Some rm -> return! localState.ReadResult rm
                | None ->
                    do! Async.Sleep 200
                    return! awaiter ()
            }

            return! awaiter()
        }
        
        member x.IncrementCompletedWorkItemCount(): Async<unit> = async {
            return! source.AsyncPost IncrementCompletedWorkItemCount
        }
        
        member x.IncrementFaultedWorkItemCount(): Async<unit> = async {
            return! source.AsyncPost IncrementFaultedWorkItemCount
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
            let localState = localStateF.Value
            let! rp = source <!- TryGetResult
            match rp with
            | None -> return None
            | Some rm ->
                let! r = localState.ReadResult(rm)
                return Some r
        }
        
        member x.TrySetResult(result: TaskResult, workerId : IWorkerId): Async<bool> = async {
            let localState = localStateF.Value
            let id = sprintf "taskResult-%s" <| mkUUID()
            let! rm = localState.CreateResult(result, allowNewSifts = false, fileName = id)
            return! source <!- fun ch -> TrySetResult(rm, workerId, ch)
        }

    /// <summary>
    ///     Creates a task entry instance in local process.
    /// </summary>
    /// <param name="stateF">Local state factory.</param>
    /// <param name="id">Task unique identifier.</param>
    /// <param name="info">Task metadata.</param>
    static member Create(stateF : LocalStateFactory, id : string, info : CloudTaskInfo) =
        let logger = stateF.Value.Logger
        let behaviour (state : TaskCompletionSourceState) (msg : TaskCompletionSourceMsg) = async {
            match msg with
            | GetState rc ->
                do! rc.Reply (TaskCompletionSourceState.ExportState state)
                return state

            | TrySetResult(_, workerId, rc) when Option.isSome state.Result ->
                do logger.Logf LogLevel.Warning "CloudTask[%s] '%s' received duplicate result from worker '%s'." info.ReturnTypeName id workerId.Id
                do! rc.Reply false
                return state

            | TrySetResult(r, workerId, rc) ->
                do logger.Logf LogLevel.Debug "CloudTask[%s] '%s' received result from worker '%s'." info.ReturnTypeName id workerId.Id
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
                                TotalWorkItemCount = state.TotalWorkItemCount + 1 ; 
                                ActiveWorkItemCount = state.ActiveWorkItemCount + 1 ;
                                MaxActiveWorkItemCount = max state.MaxActiveWorkItemCount (1 + state.ActiveWorkItemCount) }

            | IncrementCompletedWorkItemCount ->
                return { state with ActiveWorkItemCount = state.ActiveWorkItemCount - 1 ; CompletedWorkItemCount = state.CompletedWorkItemCount + 1 }

            | IncrementFaultedWorkItemCount ->
                return { state with ActiveWorkItemCount = state.ActiveWorkItemCount - 1 ; FaultedWorkItemCount = state.FaultedWorkItemCount + 1 }
        }

        let ref =
            Behavior.stateful (TaskCompletionSourceState.Init info) behaviour
            |> Actor.bind
            |> Actor.Publish
            |> Actor.ref

        new ActorTaskCompletionSource(stateF, ref, id, info)