namespace MBrace.SampleRuntime

open System

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

type private TaskEntryMsg =
    | GetState of IReplyChannel<CloudTaskState>
    | GetReturnType of IReplyChannel<Pickle<Type>>
    | TrySetResult of Pickle<TaskResult> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Pickle<TaskResult> option>
    | DeclareStatus of status:CloudTaskStatus
    | IncrementJobCount
    | DeclareCompletedJob
    | DeclareFaultedJob

type private TaskState = 
    { 
        ReturnType : Pickle<Type>
        Result : Pickle<TaskResult> option
        Info : CloudTaskInfo
        Status : CloudTaskStatus
        ActiveJobCount : int
        MaxActiveJobCount : int
        TotalJobCount : int
        CompletedJobCount : int
        FaultedJobCount : int
        ExecutionTime: ExecutionTime
    }
with 
    static member Init(info : CloudTaskInfo, returnType : Pickle<Type>) = 
        { 
            ReturnType = returnType ; Info = info ; Result = None
            TotalJobCount = 0 ; ActiveJobCount = 0 ; MaxActiveJobCount = 0 ; 
            CompletedJobCount = 0 ; FaultedJobCount = 0
            ExecutionTime = NotStarted ; Status = Posted 
        }

    static member ExportState(ts : TaskState) : CloudTaskState =
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

[<AutoSerializable(true)>]
type TaskEntry private (source : ActorRef<TaskEntryMsg>, info : CloudTaskInfo)  =
    member __.Info = info
    interface ICloudTaskEntry with
        member x.AwaitResult(): Async<TaskResult> = async {
            let rec awaiter () = async {
                let! result = source <!- TryGetResult
                match result with
                | Some r -> return Config.Serializer.UnPickleTyped r
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
        
        member x.GetReturnType(): Async<Type> = async {
            let! rtp = source <!- GetReturnType
            return Config.Serializer.UnPickleTyped rtp
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
            return rp |> Option.map Config.Serializer.UnPickleTyped
        }
        
        member x.TrySetResult(result: TaskResult): Async<bool> = async {
            let rp = Config.Serializer.PickleTyped result
            return! source <!- fun ch -> TrySetResult(rp, ch)
        }

    static member Init(pt : Pickle<Type>, info : CloudTaskInfo) =
        let behaviour (state : TaskState) (msg : TaskEntryMsg) = async {
            match msg with
            | GetState rc ->
                do! rc.Reply (TaskState.ExportState state)
                return state

            | GetReturnType rc ->
                do! rc.Reply state.ReturnType
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
            Behavior.stateful (TaskState.Init(info, pt)) behaviour
            |> Actor.bind
            |> Actor.Publish
            |> Actor.ref

        new TaskEntry(ref, info)
         

type private TaskManagerMsg =
    | CreateTaskEntry of resultType:Pickle<Type> * typeName:string * AssemblyId[] * ICloudCancellationTokenSource * taskName:string option * IReplyChannel<TaskEntry>
    | TryGetTaskCompletionSourceById of taskId:string * IReplyChannel<TaskEntry option>
    | GetAllTasks of IReplyChannel<TaskEntry []>
    | ClearAllTasks of IReplyChannel<unit>
    | ClearTask of taskId:string * IReplyChannel<bool>

type CloudTaskManager private (ref : ActorRef<TaskManagerMsg>) =
    interface ICloudTaskManager with
        member x.CreateTaskEntry(returnType : Type, dependencies: AssemblyId [], cancellationTokenSource: ICloudCancellationTokenSource, taskName: string option) = async {
            let pt = Config.Serializer.PickleTyped returnType
            let typeName = Type.prettyPrint returnType
            let! te = ref <!- fun ch -> CreateTaskEntry(pt, typeName, dependencies, cancellationTokenSource, taskName, ch)
            return te :> ICloudTaskEntry
        }

        member x.Clear(taskId: string): Async<unit> = async {
            let! found = ref <!- fun ch -> ClearTask(taskId, ch)
            return ()
        }
        
        member x.ClearAllTasks(): Async<unit> = async {
            return! ref <!- ClearAllTasks
        }
        
        member x.GetAllTasks(): Async<ICloudTaskEntry []> = async {
            let! entries = ref <!- GetAllTasks
            return entries |> Array.map unbox
        }
        
        member x.TryGetEntryById (taskId: string): Async<ICloudTaskEntry option> = async {
            let! result = ref <!- fun ch -> TryGetTaskCompletionSourceById(taskId, ch)
            return result |> Option.map unbox
        }

    static member Init() =
        let behaviour (state : Map<string, TaskEntry>) (msg : TaskManagerMsg) = async {
            match msg with
            | CreateTaskEntry(returnType, typeName, dependencies, cts, taskName, ch) ->
                let taskInfo = 
                    {
                        Id = mkUUID()
                        Name = taskName
                        Dependencies = dependencies
                        Type = typeName
                        CancellationTokenSource = cts
                    }

                let te = TaskEntry.Init(returnType, taskInfo)
                do! ch.Reply te
                return state.Add(taskInfo.Id, te)

            | TryGetTaskCompletionSourceById(taskId, ch) ->
                let result = state.TryFind taskId
                do! ch.Reply result
                return state

            | GetAllTasks rc ->
                do! rc.Reply (state |> Seq.map (fun kv -> kv.Value) |> Seq.toArray)
                return state

            | ClearAllTasks rc ->
                do for KeyValue(_,ts) in state do
                    ts.Info.CancellationTokenSource.Cancel()

                do! rc.Reply()

                return Map.empty

            | ClearTask (taskId, rc) ->
                match state.TryFind taskId with
                | None ->
                    do! rc.Reply false
                    return state
                | Some ts ->
                    ts.Info.CancellationTokenSource.Cancel()
                    do! rc.Reply true
                    return state.Remove taskId
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new CloudTaskManager(ref)