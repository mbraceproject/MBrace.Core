namespace MBrace.Thespian

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Runtime

type private CancellationEntryMsg =
    | IsCancellationRequested of IReplyChannel<bool>
    | RegisterChild of child:CancellationEntry * IReplyChannel<bool>
    | Cancel

and CancellationEntry private (id : string, source : ActorRef<CancellationEntryMsg>) =
    member __.Id = id
    member __.Cancel () = source.AsyncPost Cancel
    member __.IsCancellationRequested = source <!- IsCancellationRequested
    member __.RegisterChild c = source <!- fun ch -> RegisterChild(c,ch)
    
    interface ICancellationEntry with
        member __.UUID = id
        member __.Cancel() = __.Cancel()
        member __.IsCancellationRequested = __.IsCancellationRequested
        member __.Dispose () = async.Zero()

    static member Init() =
        let behaviour (state : Map<string, CancellationEntry> option) (msg : CancellationEntryMsg) = async {
            match msg, state with
            | IsCancellationRequested rc, _ ->
                do! rc.Reply (Option.isNone state)
                return state
            // has been cancelled, return false
            | RegisterChild (child, rc), None ->
                do! rc.Reply false
                return state
            // cancellation token active, register normally
            | RegisterChild (child, rc), Some children ->
                do! rc.Reply true
                return Some <| children.Add(child.Id, child)

            // token is already canceled, nothing to do
            | Cancel, None -> return None
            // token canceled, cancel children and update state
            | Cancel, Some children ->
                do! 
                    children
                    |> Seq.map (fun kv -> kv.Value.Cancel())
                    |> Async.Parallel
                    |> Async.Ignore

                return None
        }

        let id = mkUUID()
        let aref =
            Actor.Stateful (Some Map.empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new CancellationEntry(id, aref)