namespace MBrace.Thespian.Runtime

open System

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters      

type private TaskManagerMsg =
    | CreateTaskEntry of info:CloudTaskInfo * IReplyChannel<ActorTaskCompletionSource>
    | TryGetTaskCompletionSourceById of taskId:string * IReplyChannel<ActorTaskCompletionSource option>
    | GetAllTasks of IReplyChannel<ActorTaskCompletionSource []>
    | ClearAllTasks of IReplyChannel<unit>
    | ClearTask of taskId:string * IReplyChannel<bool>

type CloudTaskManager private (ref : ActorRef<TaskManagerMsg>) =
    interface ICloudTaskManager with
        member x.CreateTask(info : CloudTaskInfo) = async {
            let! te = ref <!- fun ch -> CreateTaskEntry(info, ch)
            return te :> ICloudTaskCompletionSource
        }

        member x.Clear(taskId: string): Async<unit> = async {
            let! found = ref <!- fun ch -> ClearTask(taskId, ch)
            return ()
        }
        
        member x.ClearAllTasks(): Async<unit> = async {
            return! ref <!- ClearAllTasks
        }
        
        member x.GetAllTasks(): Async<ICloudTaskCompletionSource []> = async {
            let! entries = ref <!- GetAllTasks
            return entries |> Array.map unbox
        }
        
        member x.TryGetTaskById (taskId: string): Async<ICloudTaskCompletionSource option> = async {
            let! result = ref <!- fun ch -> TryGetTaskCompletionSourceById(taskId, ch)
            return result |> Option.map unbox
        }

    static member Init() =
        let behaviour (state : Map<string, ActorTaskCompletionSource>) (msg : TaskManagerMsg) = async {
            match msg with
            | CreateTaskEntry(info, ch) ->
                let id = mkUUID()
                let te = ActorTaskCompletionSource.Create(id, info)
                do! ch.Reply te
                return state.Add(te.Id, te)

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