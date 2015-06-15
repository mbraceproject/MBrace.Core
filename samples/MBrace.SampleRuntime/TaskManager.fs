namespace MBrace.SampleRuntime

open System

open Nessos.Thespian
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

type private TaskManagerMsg =
    | RequestTaskCompletionSource of Existential * AssemblyId[] * ICloudCancellationTokenSource * taskName:string option * IReplyChannel<ICloudTaskCompletionSource>
    | TryGetTaskCompletionSourceById of taskId:string * IReplyChannel<ICloudTaskCompletionSource option>
    | DeclareStatus of taskId:string * status:CloudTaskStatus
    | IncrementJobCount of taskId:string
    | DecrementJobCount of taskId:string
    | GetTaskState of taskId:string * IReplyChannel<CloudTaskState>
    | GetAllTasks of IReplyChannel<CloudTaskState []>
    | ClearAllTasks of IReplyChannel<unit>
    | ClearTask of taskId:string * IReplyChannel<bool>

type ExecutionTime =
    | NotStarted
    | Started of DateTime
    | Finished of DateTime * TimeSpan

type private TaskState = 
    { 
        TaskCompletionSource : ICloudTaskCompletionSource
        Status : CloudTaskStatus
        ActiveJobCount : int
        MaxActiveJobCount : int
        TotalJobCount : int
        ExecutionTime: ExecutionTime
    }
with 
    static member Init(tcs) = { TaskCompletionSource = tcs ; TotalJobCount = 0 ; ActiveJobCount = 0 ; MaxActiveJobCount = 0 ; ExecutionTime = NotStarted ; Status = Posted }
    static member ExportState(ts : TaskState) =
        {
            Info = ts.TaskCompletionSource.Info
            Status = ts.Status
            ActiveJobCount = ts.ActiveJobCount
            TotalJobCount = ts.TotalJobCount
            MaxActiveJobCount = ts.MaxActiveJobCount
            ExecutionTime = 
                match ts.ExecutionTime with 
                | NotStarted -> None
                | Started t -> Some (t, DateTime.Now - t)
                | Finished (t,s) -> Some(t,s)
        }

type private TaskManagerState = Map<string, TaskState>

type CloudTaskManager private (ref : ActorRef<TaskManagerMsg>) =
    interface ICloudTaskManager with
        member x.Clear(taskId: string): Async<unit> = async {
            let! found = ref <!- fun ch -> ClearTask(taskId, ch)
            return
                if found then ()
                else
                    invalidOp <| sprintf "Could not locate task of id '%s'." taskId
        }
        
        member x.ClearAllTasks(): Async<unit> = async {
            return! ref <!- ClearAllTasks
        }
        
        member x.DeclareStatus(taskId: string, status: CloudTaskStatus): Async<unit> = async {
            return! ref.AsyncPost <| DeclareStatus(taskId, status)   
        }
        
        member x.IncrementJobCount(taskId: string): Async<unit> = async {
            return! ref.AsyncPost <| IncrementJobCount taskId
        }
        
        member x.DecrementJobCount(taskId: string): Async<unit> = async {
            return! ref.AsyncPost <| DecrementJobCount taskId
        }
        
        member x.GetAllTasks(): Async<CloudTaskState []> = async {
            return! ref <!- GetAllTasks
        }
        
        member x.GetTaskCompletionSourceById(taskId: string): Async<ICloudTaskCompletionSource> = async {
            let! result = ref <!- fun ch -> TryGetTaskCompletionSourceById(taskId, ch)
            return
                match result with
                | None -> invalidOp "Could not locate task of id '%s'." taskId
                | Some r -> r
        }
        
        member x.GetTaskState(taskId: string): Async<CloudTaskState> = async {
            return! ref <!- fun ch -> GetTaskState(taskId, ch)
        }
        
        member x.RequestTaskCompletionSource<'T>(dependencies: AssemblyId [], cancellationTokenSource: ICloudCancellationTokenSource, taskName: string option): Async<ICloudTaskCompletionSource<'T>> = async {
            let e = new Existential<'T> ()
            let! tcs = ref <!- fun ch -> RequestTaskCompletionSource(e, dependencies, cancellationTokenSource, taskName, ch)
            return tcs :?> ICloudTaskCompletionSource<'T>
        }

    static member Init() =
        let behaviour (state : TaskManagerState) (msg : TaskManagerMsg) = async {
            match msg with
            | RequestTaskCompletionSource(e, dependencies, cts, taskName, ch) ->
                let tcs = 
                    e.Apply {
                        new IFunc<ICloudTaskCompletionSource> with
                            member __.Invoke<'T>() =
                                let taskInfo = 
                                    {
                                        Id = mkUUID()
                                        Name = taskName
                                        Dependencies = dependencies
                                        Type = Type.prettyPrint typeof<'T>
                                    }

                                TaskCompletionSource<'T>.Init(cts, taskInfo) :> ICloudTaskCompletionSource
                    }

                do! ch.Reply tcs
                return state.Add(tcs.Info.Id, TaskState.Init tcs)

            | TryGetTaskCompletionSourceById (taskId, ch) ->
                let result = state.TryFind taskId
                do! ch.Reply (result |> Option.map (fun s -> s.TaskCompletionSource))
                return state

            | DeclareStatus (taskId, status) ->
                match state.TryFind taskId with
                | None -> return state
                | Some ts -> 
                    let executionTime =
                        match ts.ExecutionTime, status with
                        | NotStarted, Running -> Started DateTime.Now
                        | Started t, (Completed | UserException | Canceled) -> Finished(t, DateTime.Now - t)
                        | et, _ -> et

                    return state.Add(taskId, { ts with Status = status ; ExecutionTime = executionTime })

            | IncrementJobCount taskId ->
                match state.TryFind taskId with
                | None -> return state
                | Some ts -> 
                    return state.Add(taskId, { ts with 
                                                    TotalJobCount = ts.TotalJobCount + 1 ; 
                                                    ActiveJobCount = ts.ActiveJobCount + 1 ;
                                                    MaxActiveJobCount = max ts.MaxActiveJobCount (1 + ts.ActiveJobCount) })

            | DecrementJobCount taskId ->
                match state.TryFind taskId with
                | None -> return state
                | Some ts -> return state.Add(taskId, { ts with ActiveJobCount = ts.ActiveJobCount - 1 })

            | GetTaskState (taskId, rc) ->
                match state.TryFind taskId with
                | None -> return state
                | Some ts -> 
                    do! rc.Reply(TaskState.ExportState ts)
                    return state

            | GetAllTasks rc ->
                let tasks = state |> Seq.map (fun kv -> TaskState.ExportState kv.Value) |> Seq.toArray
                do! rc.Reply tasks
                return state

            | ClearAllTasks rc ->
                do for KeyValue(_,ts) in state do
                    ts.TaskCompletionSource.CancellationTokenSource.Cancel()

                do! rc.Reply()

                return Map.empty

            | ClearTask (taskId, rc) ->
                match state.TryFind taskId with
                | None ->
                    do! rc.Reply false
                    return state
                | Some ts ->
                    ts.TaskCompletionSource.CancellationTokenSource.Cancel()
                    do! rc.Reply true
                    return state.Remove taskId
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new CloudTaskManager(ref)