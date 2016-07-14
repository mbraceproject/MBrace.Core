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
    | CreateProcessEntry of info:CloudProcessInfo * IReplyChannel<ActorProcessEntry>
    | TryGetProcessCompletionSourceById of procId:string * IReplyChannel<ActorProcessEntry option>
    | GetAllProcesses of IReplyChannel<ActorProcessEntry []>
    | ClearAllProcesses of IReplyChannel<unit>
    | ClearProcess of procId:string * IReplyChannel<bool>

///  Task manager actor reference used for handling MBrace.Thespian task instances
[<Sealed; AutoSerializable(true)>]
type CloudProcessManager private (ref : ActorRef<ProcessManagerMsg>) =
    interface ICloudProcessManager with
        member x.StartProcess(info : CloudProcessInfo) = async {
            let! te = ref <!- fun ch -> CreateProcessEntry(info, ch)
            return te :> ICloudProcessEntry
        }

        member x.ClearProcess(procId: string): Async<unit> = async {
            let! _ = ref <!- fun ch -> ClearProcess(procId, ch)
            return ()
        }
        
        member x.ClearAllProcesses(): Async<unit> = async {
            return! ref <!- ClearAllProcesses
        }
        
        member x.GetAllProcesses(): Async<ICloudProcessEntry []> = async {
            let! entries = ref <!- GetAllProcesses
            return entries |> Array.map unbox
        }
        
        member x.TryGetProcessById (procId: string): Async<ICloudProcessEntry option> = async {
            let! result = ref <!- fun ch -> TryGetProcessCompletionSourceById(procId, ch)
            return result |> Option.map unbox
        }

    /// <summary>
    ///     Creates a new Task Manager instance running in the local process.
    /// </summary>
    static member Create(localStateF : LocalStateFactory) =
        let logger = localStateF.Value.Logger
        let behaviour (state : Map<string, ActorProcessEntry>) (msg : ProcessManagerMsg) = async {
            match msg with
            | CreateProcessEntry(info, ch) ->
                let id = mkUUID()
                let te = ActorProcessEntry.Create(localStateF, id, info)
                logger.Logf LogLevel.Debug "ProcessManager has created a new task completion source '%s' of type '%s'." te.Id te.Info.ReturnTypeName
                do! ch.Reply te
                return state.Add(te.Id, te)

            | TryGetProcessCompletionSourceById(procId, ch) ->
                let result = state.TryFind procId
                do! ch.Reply result
                return state

            | GetAllProcesses rc ->
                do! rc.Reply (state |> Seq.map (fun kv -> kv.Value) |> Seq.toArray)
                return state

            | ClearAllProcesses rc ->
                do for KeyValue(_,ts) in state do
                    ts.Info.CancellationTokenSource.Cancel()

                do! rc.Reply()

                logger.Logf LogLevel.Debug "Clearing all ProcessManager state."
                return Map.empty

            | ClearProcess (procId, rc) ->
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
