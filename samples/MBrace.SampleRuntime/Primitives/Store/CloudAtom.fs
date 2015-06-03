namespace MBrace.SampleRuntime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

type private tag = uint64

type private AtomMsg<'T> =
    | GetValue of IReplyChannel<tag * 'T>
    | TrySetValue of tag * 'T * IReplyChannel<bool>
    | ForceValue of 'T * IReplyChannel<unit>
    | Dispose of IReplyChannel<unit>

type Atom<'T> private (id : string, source : ActorRef<AtomMsg<'T>>) =

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = Cloud.OfAsync <| async {
            let! _,value = source <!- GetValue
            return value
        }

        member __.Dispose() = Cloud.OfAsync <| async { return! source <!- Dispose }

        member __.Force(value : 'T) = Cloud.OfAsync <| async { return! source <!- fun ch -> ForceValue(value, ch) }
        member __.Transact(f : 'T -> 'R * 'T, ?maxRetries) = Cloud.OfAsync <| async {
            if maxRetries |> Option.exists (fun i -> i < 0) then
                invalidArg "maxRetries" "must be non-negative."

            let cell = ref Unchecked.defaultof<'R>
            let rec tryUpdate retries = async {
                let! tag, value = source <!- GetValue
                let r, value' = f value
                cell := r
                let! success = source <!- fun ch -> TrySetValue(tag, value', ch)
                if success then return ()
                else
                    match maxRetries with
                    | None -> return! tryUpdate None
                    | Some 0 -> return raise <| new OperationCanceledException("ran out of retries.")
                    | Some i -> return! tryUpdate (Some (i-1))
            }

            do! tryUpdate maxRetries
            return cell.Value
        }

    static member Init(id : string, init : 'T) =
        let behaviour (state : (uint64 * 'T) option) (msg : AtomMsg<'T>) = async {
            match state with
            | None -> // object disposed
                let e = new System.ObjectDisposedException("ActorAtom")
                match msg with
                | GetValue rc -> do! rc.ReplyWithException e
                | TrySetValue(_,_,rc) -> do! rc.ReplyWithException e
                | ForceValue(_,rc) -> do! rc.ReplyWithException e
                | Dispose rc -> do! rc.ReplyWithException e
                return state

            | Some ((tag, value) as s) ->
                match msg with
                | GetValue rc ->
                    do! rc.Reply s
                    return state
                | TrySetValue(tag', value', rc) ->
                    if tag' = tag then
                        do! rc.Reply true
                        return Some (tag + 1uL, value')
                    else
                        do! rc.Reply false
                        return state
                | ForceValue(value', rc) ->
                    do! rc.Reply ()
                    return Some (tag + 1uL, value')
                | Dispose rc ->
                    do! rc.Reply ()
                    return None
        }

        let ref =
            Actor.Stateful (Some (0uL, init)) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Atom<'T>(id, ref)


type ActorAtomProvider (factory : ResourceFactory) =
    let id = mkUUID()
    interface ICloudAtomProvider with
        member x.CreateAtom(container: string, initValue: 'T): Async<ICloudAtom<'T>> = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! atom = factory.RequestResource(fun () -> Atom<'T>.Init(id, initValue))
            return atom :> ICloudAtom<'T>
        }

        member x.CreateUniqueContainerName () = System.Guid.NewGuid().ToString()

        member x.DisposeContainer (_ : string) = async.Zero()
        
        member x.IsSupportedValue(value: 'T): bool = true
        
        member x.Name: string = "ActorAtom"
        member x.Id = id