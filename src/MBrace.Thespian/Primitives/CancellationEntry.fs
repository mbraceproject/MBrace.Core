namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private CancellationEntryMsg =
    | IsCancellationRequested of IReplyChannel<bool>
    | RegisterChild of child:ActorCancellationEntry * IReplyChannel<bool>
    | Cancel

/// Defines a cancellable entity with linking support
and [<Sealed; AutoSerializable(true)>] ActorCancellationEntry private (id : string, source : ActorRef<CancellationEntryMsg>) =
    member __.Id = id
    member __.Cancel () = source.AsyncPost Cancel
    member __.IsCancellationRequested = source <!- IsCancellationRequested

    /// <summary>
    ///     Registers a cancellation entry as child to the current entry.
    ///     Returns true if successful, false if this entry was already cancelled.
    /// </summary>
    /// <param name="child">Child to be registered.</param>
    member __.RegisterChild (child : ActorCancellationEntry) = source <!- fun ch -> RegisterChild(child,ch)
    
    interface ICancellationEntry with
        member __.UUID = id
        member __.Cancel() = __.Cancel()
        member __.IsCancellationRequested = __.IsCancellationRequested
        member __.Dispose () = async.Zero()

    /// Creates a cancellable actor instance that runs in the local process
    static member Create() =
        let behaviour (state : Map<string, ActorCancellationEntry> option) (msg : CancellationEntryMsg) = async {
            match msg, state with
            | IsCancellationRequested rc, _ ->
                do! rc.Reply (Option.isNone state)
                return state
            // has been cancelled, return false
            | RegisterChild (_, rc), None ->
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

        new ActorCancellationEntry(id, aref)

/// Global actor cancellation entry factory
[<Sealed; AutoSerializable(true)>]
type ActorCancellationEntryFactory(factory : ResourceFactory) =
    interface ICancellationEntryFactory with
        member x.TryCreateCancellationEntry(parents: ICancellationEntry []) = async {
            let parents = parents |> Array.map unbox<ActorCancellationEntry>
            let! e = factory.RequestResource(fun () -> ActorCancellationEntry.Create())
            let! results =
                parents
                |> Seq.map (fun p -> p.RegisterChild e)
                |> Async.Parallel

            // cancel the token if any of the parents have been cancelled
            if Array.forall id results 
            then return Some(e :> ICancellationEntry)
            else let! _ = e.Cancel() in return None
        }