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
open MBrace.Runtime.Store

type private ProcessManagerMsg =
    | CreateCloudProcessEntry of info:CloudProcessInfo * IReplyChannel<ActorCompletionSource>
    | TryGetCloudProcessCompletionSourceById of procId:string * IReplyChannel<ActorCompletionSource option>
    | GetAllJobs of IReplyChannel<ActorCompletionSource []>
    | ClearAllJobs of IReplyChannel<unit>
    | ClearJob of procId:string * IReplyChannel<bool>

///  Task manager actor reference used for handling MBrace.Thespian task instances
type CloudProcessManager private (ref : ActorRef<ProcessManagerMsg>) =
    interface ICloudProcessManager with
        member x.StartJob(info : CloudProcessInfo) = async {
            let! te = ref <!- fun ch -> CreateCloudProcessEntry(info, ch)
            return te :> ICloudProcessCompletionSource
        }

        member x.Clear(procId: string): Async<unit> = async {
            let! found = ref <!- fun ch -> ClearJob(procId, ch)
            return ()
        }
        
        member x.ClearAllJobs(): Async<unit> = async {
            return! ref <!- ClearAllJobs
        }
        
        member x.GetAllJobs(): Async<ICloudProcessCompletionSource []> = async {
            let! entries = ref <!- GetAllJobs
            return entries |> Array.map unbox
        }
        
        member x.TryGetCloudProcess (procId: string): Async<ICloudProcessCompletionSource option> = async {
            let! result = ref <!- fun ch -> TryGetCloudProcessCompletionSourceById(procId, ch)
            return result |> Option.map unbox
        }

    /// <summary>
    ///     Creates a new Task Manager instance running in the local process.
    /// </summary>
    static member Create(localStateF : LocalStateFactory) =
        let logger = localStateF.Value.Logger
        let behaviour (state : Map<string, ActorCompletionSource>) (msg : ProcessManagerMsg) = async {
            match msg with
            | CreateCloudProcessEntry(info, ch) ->
                let id = mkUUID()
                let te = ActorCompletionSource.Create(localStateF, id, info)
                logger.Logf LogLevel.Debug "ProcessManager has created a new task completion source '%s' of type '%s'." te.Id te.Info.ReturnTypeName
                do! ch.Reply te
                return state.Add(te.Id, te)

            | TryGetCloudProcessCompletionSourceById(procId, ch) ->
                let result = state.TryFind procId
                do! ch.Reply result
                return state

            | GetAllJobs rc ->
                do! rc.Reply (state |> Seq.map (fun kv -> kv.Value) |> Seq.toArray)
                return state

            | ClearAllJobs rc ->
                do for KeyValue(_,ts) in state do
                    ts.Info.CancellationTokenSource.Cancel()

                do! rc.Reply()

                logger.Logf LogLevel.Debug "Clearing all ProcessManager state."
                return Map.empty

            | ClearJob (procId, rc) ->
                match state.TryFind procId with
                | None ->
                    do! rc.Reply false
                    return state
                | Some ts ->
                    ts.Info.CancellationTokenSource.Cancel()
                    do! rc.Reply true
                    return state.Remove procId
        }

        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new CloudProcessManager(ref)
