namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals

[<AutoOpen>]
module private ActorCloudDictionary =

    type Tag = int

    [<Literal>]
    let tag0 = 0 // initial tag, reserved for keys missing from dictionary

    type CloudDictionaryMsg =
        | TryAdd of key:string * value:byte[] * IReplyChannel<Tag option>
        | TransactAdd of key:string * value:byte[] * tag:Tag * IReplyChannel<Tag option>
        | ForceAdd of key:string * value:byte[] * IReplyChannel<Tag>
        | TryGetValue of key:string * IReplyChannel<Tag * byte[] option>
        | ContainsKey of key:string * IReplyChannel<bool>
        | Remove of key:string * IReplyChannel<bool>
        | GetCount of IReplyChannel<int64>
        | ToArray of IReplyChannel<(string * byte[]) []>

    /// Creates a CloudDictionary actor instance in the local process
    let init () =
        let behaviour (state : Map<string, Tag * byte[]>) (msg : CloudDictionaryMsg) = async {
            match msg with
            | TryAdd(key, value, rc) when state.ContainsKey key ->
                do! rc.Reply None
                return state

            | TryAdd(key, value, rc) ->
                let tag = tag0 + 1
                do! rc.Reply (Some tag)
                return state.Add(key, (tag, value))

            | TransactAdd(key, value, tag, rc) ->
                match state.TryFind key with
                | None when tag = tag0 ->
                    let tag' = tag + 1
                    do! rc.Reply (Some tag')
                    return state.Add(key, (tag', value))
                | Some(tag', _) when tag' = tag ->
                    let tag'' = tag + 1
                    do! rc.Reply (Some tag'')
                    return state.Add(key, (tag'', value))
                | _ ->
                    do! rc.Reply None
                    return state

            | ForceAdd(key, value, rc) ->
                let tag = match state.TryFind key with Some(t,_) -> t + 1 | None -> tag0 + 1
                do! rc.Reply tag
                return state.Add(key, (tag, value))

            | TryGetValue(key, rc) ->
                match state.TryFind key with
                | None -> do! rc.Reply (tag0, None)
                | Some(t,v) -> do! rc.Reply (t, Some v)

                return state

            | ContainsKey(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state
            | Remove(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state.Remove key
            | GetCount rc ->
                do! rc.Reply (int64 state.Count)
                return state
            | ToArray rc ->
                do! rc.Reply (state |> Seq.map (fun kv -> kv.Key, snd kv.Value) |> Seq.toArray)
                return state
        }

        Actor.Stateful Map.empty behaviour
        |> Actor.Publish
        |> Actor.ref

    /// Serializable actor ICloudDictionary implementation
    [<Sealed; AutoSerializable(true)>]
    type ActorCloudDictionary<'T> internal (id : string, source : ActorRef<CloudDictionaryMsg>) =
        static let pickle t = Config.Serializer.Pickle t
        static let unpickle bytes = Config.Serializer.UnPickle<'T> bytes

        let toEnum () = async {
            let! pairs = source <!- ToArray
            return pairs |> Seq.map (fun (k,b) -> new KeyValuePair<_,_>(k, unpickle b))
        }

        interface seq<KeyValuePair<string,'T>> with
            member x.GetEnumerator() = Async.RunSync(toEnum()).GetEnumerator() :> Collections.IEnumerator
            member x.GetEnumerator() = Async.RunSync(toEnum()).GetEnumerator()
        
        interface CloudDictionary<'T> with
            member x.Add(key: string, value: 'T): Async<unit> = async { 
                let! _ = source <!- fun ch -> ForceAdd(key, pickle value, ch) in return () 
            }
        
            member x.Transact(key: string, updater: 'T option -> 'R * 'T, ?maxRetries:int): Async<'R> = async { 
                if maxRetries |> Option.exists (fun i -> i < 0) then
                    invalidArg "maxRetries" "must be non-negative."

                let cell = ref Unchecked.defaultof<'R>
                let rec tryUpdate retries = async {
                    let! tag, bytes = source <!- fun ch -> TryGetValue(key, ch)
                    let value = bytes |> Option.map unpickle
                    let r, value' = updater value
                    cell := r
                    let! tag' = source <!- fun ch -> TransactAdd(key, pickle value', tag, ch)
                    match tag' with
                    | Some t -> return ()
                    | None ->
                        match maxRetries with
                        | None -> return! tryUpdate None
                        | Some 0 -> return raise <| new OperationCanceledException("ran out of retries.")
                        | Some i -> return! tryUpdate (Some (i-1))
                }

                do! tryUpdate maxRetries
                return cell.Value
            }
        
            member x.ContainsKey(key: string): Async<bool> = 
                async { return! source <!- fun ch -> ContainsKey(key, ch) }
        
            member x.Id: string = id
        
            member x.Remove(key: string): Async<bool> = 
                async { return! source <!- fun ch -> Remove(key, ch) }
        
            member x.TryAdd(key: string, value: 'T): Async<bool> = 
                async { let! tag = source <!- fun ch -> TryAdd(key, pickle value, ch) in return Option.isSome tag }
        
            member x.TryFind(key: string): Async<'T option> = async { 
                let! _,bytes = source <!- fun ch -> TryGetValue(key, ch)
                return bytes |> Option.map unpickle
            }

        interface ICloudCollection<KeyValuePair<string,'T>> with
            member x.GetCount(): Async<int64> = async {
                return! source <!- GetCount
            }

            member x.IsKnownSize = true
            member x.IsKnownCount = true
            member x.IsMaterialized = false

            member x.GetSize(): Async<int64> = async {
                return! source <!- GetCount
            }

            member x.ToEnumerable() = toEnum ()

        interface ICloudDisposable with
            member x.Dispose(): Async<unit> = async.Zero ()

/// Defines a distributed cloud channel factory
[<Sealed; AutoSerializable(true)>]
type ActorDictionaryProvider (factory : ResourceFactory) =
    let id = sprintf "actorDictProvider-%s" <| mkUUID()
    interface ICloudDictionaryProvider with
        member __.Name = "MBrace.Thespian Actor CloudDictionary Provider"
        member __.Id = id
        member __.IsSupportedValue _ = true
        member __.GetRandomDictionaryId() = sprintf "actorDict-%s" <| mkUUID()
        member __.CreateDictionary<'T> (dictId : string) : Async<CloudDictionary<'T>> = async {
            let! actor = factory.RequestResource(fun () -> ActorCloudDictionary.init())
            let dict = new ActorCloudDictionary<'T>(dictId, actor)
            return dict :> CloudDictionary<'T>
        }

        member __.GetById<'T> (dictId : string) : Async<CloudDictionary<'T>> =
            raise (new NotSupportedException("Named CloudAtom lookup not supported in Thespian atoms."))