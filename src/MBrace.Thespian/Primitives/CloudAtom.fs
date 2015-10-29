namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals

[<AutoOpen>]
module private ActorAtom =

    type tag = uint64

    type AtomMsg =
        | GetValue of IReplyChannel<tag * byte[]>
        | TrySetValue of tag * byte[] * IReplyChannel<bool>
        | ForceValue of byte[] * IReplyChannel<unit>
        | Dispose of IReplyChannel<unit>

    /// <summary>
    ///     Initializes an actor atom with provided id and initial value.
    /// </summary>
    /// <param name="id"></param>
    /// <param name="initialValue"></param>
    let init (id : string) (initialValue : byte[]) : ActorRef<AtomMsg> =
        let behaviour (state : (uint64 * byte[]) option) (msg : AtomMsg) = async {
            match state with
            | None -> // object disposed
                let e = new System.ObjectDisposedException("ActorAtom")
                match msg with
                | GetValue rc -> do! rc.ReplyWithException e
                | TrySetValue(_,_,rc) -> do! rc.ReplyWithException e
                | ForceValue(_,rc) -> do! rc.ReplyWithException e
                | Dispose rc -> do! rc.ReplyWithException e
                return state

            | Some ((tag, bytes) as s) ->
                match msg with
                | GetValue rc ->
                    do! rc.Reply s
                    return state
                | TrySetValue(tag', bytes', rc) ->
                    if tag' = tag then
                        do! rc.Reply true
                        return Some (tag + 1uL, bytes')
                    else
                        do! rc.Reply false
                        return state
                | ForceValue(bytes', rc) ->
                    do! rc.Reply ()
                    return Some (tag + 1uL, bytes')
                | Dispose rc ->
                    do! rc.Reply ()
                    return None
        }

        Actor.Stateful (Some (0uL, initialValue)) behaviour
        |> Actor.Publish
        |> Actor.ref

    /// Actor atom interface implementation
    [<Sealed; AutoSerializable(true)>]
    type ActorAtom<'T> internal (id : string, container : string, source : ActorRef<AtomMsg>) =
        static let pickle (t : 'T) = Config.Serializer.Pickle t
        static let unpickle (bytes : byte[]) = Config.Serializer.UnPickle<'T> bytes

        let getValue() = async {
            let! _,bytes = source <!- GetValue
            return unpickle bytes
        }

        interface CloudAtom<'T> with
            member __.Id = id
            member __.Container = container
            member __.Value = getValue() |> Async.RunSync
            member __.GetValueAsync() = getValue()

            member __.Dispose() = async { return! source <!- Dispose }

            member __.ForceAsync(value : 'T) = async {
                return! source <!- fun ch -> ForceValue(pickle value, ch) 
            }

            member __.TransactAsync(f : 'T -> 'R * 'T, ?maxRetries) : Async<'R> = async {
                if maxRetries |> Option.exists (fun i -> i < 0) then
                    invalidArg "maxRetries" "must be non-negative."

                let cell = ref Unchecked.defaultof<'R>
                let rec tryUpdate retries = async {
                    let! tag, bytes = source <!- GetValue
                    let value = unpickle bytes
                    let r, value' = f value
                    cell := r
                    let! success = source <!- fun ch -> TrySetValue(tag, pickle value', ch)
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


/// Defines a distributed cloud atom factory
type ActorAtomProvider (factory : ResourceFactory) =
    let id = sprintf "actorAtomProvider-%s" <| mkUUID()

    interface ICloudAtomProvider with
        member x.Name: string = "MBrace.Thespian Actor CloudAtom Provider"
        member x.Id = id
        member x.IsSupportedValue(value: 'T): bool = true
        member x.DefaultContainer = "<Default CloudAtom Container>"
        member x.WithDefaultContainer _ = x :> _
        member x.GetRandomContainerName () = "<Default CloudAtom Container>"
        member x.GetRandomAtomIdentifier() = sprintf "actorAtom-%s" <| mkUUID()
        member x.DisposeContainer (_ : string) = async.Zero()
        member x.CreateAtom(container: string, atomId : string, initValue: 'T): Async<CloudAtom<'T>> = async {
            let initPickle = Config.Serializer.Pickle initValue
            let! actor = factory.RequestResource(fun () -> ActorAtom.init atomId initPickle)
            return new ActorAtom<'T>(atomId, container, actor) :> CloudAtom<'T>
        }

        member x.GetAtomById(_container : string, _atomId : string) : Async<CloudAtom<'T>> =
            raise (new NotSupportedException("Named CloudAtom lookup not supported in Thespian atoms."))