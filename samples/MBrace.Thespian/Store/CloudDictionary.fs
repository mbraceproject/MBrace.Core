namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Store
open MBrace.Store.Internals

type private CloudDictionaryMsg<'T> =
    | Add of key:string * value:'T * force:bool * IReplyChannel<bool>
    | AddOrUpdate of key:string * updater:('T option -> 'T) * IReplyChannel<'T>
    | Update of key:string * updater:('T -> 'T) * IReplyChannel<'T>
    | ContainsKey of key:string * IReplyChannel<bool>
    | Remove of key:string * IReplyChannel<bool>
    | TryFind of key:string * IReplyChannel<'T option>
    | GetCount of IReplyChannel<int64>
    | ToArray of IReplyChannel<KeyValuePair<string, 'T> []>

type CloudDictionary<'T> private (id : string, source : ActorRef<CloudDictionaryMsg<'T>>) =
        
    interface ICloudDictionary<'T> with
        member x.Add(key: string, value: 'T): Async<unit> = 
            async { let! _ = source <!- fun ch -> Add(key, value, true, ch) in return () }
        
        member x.AddOrUpdate(key: string, updater: 'T option -> 'T): Async<'T> = 
            async { return! source <!- fun ch -> AddOrUpdate(key, updater, ch) }

        member x.Update(key : string, updater : 'T -> 'T) : Async<'T> =
            async { return! source <!- fun ch -> Update(key, updater, ch) }
        
        member x.ContainsKey(key: string): Async<bool> = 
            async { return! source <!- fun ch -> ContainsKey(key, ch) }
        
        member x.Id: string = id
        
        member x.Remove(key: string): Async<bool> = 
            async { return! source <!- fun ch -> Remove(key, ch) }
        
        member x.TryAdd(key: string, value: 'T): Async<bool> = 
            async { return! source <!- fun ch -> Add(key, value, false, ch) }
        
        member x.TryFind(key: string): Async<'T option> = 
            async { return! source <!- fun ch -> TryFind(key, ch) }

    interface ICloudCollection<KeyValuePair<string,'T>> with
        member x.Count: Local<int64> = local {
            return! source <!- GetCount
        }

        member x.IsKnownSize = true
        member x.IsKnownCount = true

        member x.Size : Local<int64> = local {
            return! source <!- GetCount
        }

        member x.ToEnumerable() = local {
            let! pairs = source <!- ToArray
            return pairs :> seq<_>
        }

    interface ICloudDisposable with
        member x.Dispose(): Local<unit> = local.Zero ()

    static member Init() =
        let behaviour (state : Map<string, 'T>) (msg : CloudDictionaryMsg<'T>) = async {
            match msg with
            | Add(key, value, false, rc) when state.ContainsKey key ->
                do! rc.Reply false
                return state
            | Add(key, value, _, rc) ->
                do! rc.Reply true
                return state.Add(key, value)
            | AddOrUpdate(key, updater, rc) ->
                let t = updater (state.TryFind key)
                do! rc.Reply t
                return state.Add(key, t)
            | Update(key, updater, rc) ->
                match state.TryFind key with
                | None ->
                    do! rc.ReplyWithException(new KeyNotFoundException(key))
                    return state
                | Some t ->
                    do! rc.Reply t
                    return state.Add(key, t)

            | ContainsKey(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state
            | Remove(key, rc) ->
                do! rc.Reply (state.ContainsKey key)
                return state.Remove key
            | TryFind(key, rc) ->
                do! rc.Reply (state.TryFind key)
                return state
            | GetCount rc ->
                do! rc.Reply (int64 state.Count)
                return state
            | ToArray rc ->
                do! rc.Reply (state |> Seq.toArray)
                return state
        }

        let id = Guid.NewGuid().ToString()
        let ref =
            Actor.Stateful Map.empty behaviour
            |> Actor.Publish
            |> Actor.ref

        new CloudDictionary<'T>(id, ref)

type ActorDictionaryProvider (factory : ResourceFactory) =
    interface ICloudDictionaryProvider with
        member __.IsSupportedValue _ = true
        member __.Create<'T> () = async {
            let! dict = factory.RequestResource(fun () -> CloudDictionary<'T>.Init())
            return dict :> ICloudDictionary<'T>
        }