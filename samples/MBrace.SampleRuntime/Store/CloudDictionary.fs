namespace MBrace.SampleRuntime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Store

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
    let (<!-) ar msgB = local { return! Cloud.OfAsync(ar <!- msgB)}
    interface ICloudDictionary<'T> with
        member x.Add(key: string, value: 'T): Local<unit> = 
            local { let! _ = source <!- fun ch -> Add(key, value, true, ch) in return () }
        
        member x.AddOrUpdate(key: string, updater: 'T option -> 'T): Local<'T> = 
            source <!- fun ch -> AddOrUpdate(key, updater, ch)

        member x.Update(key : string, updater : 'T -> 'T) : Local<'T> =
            source <!- fun ch -> Update(key, updater, ch)
        
        member x.ContainsKey(key: string): Local<bool> = 
            source <!- fun ch -> ContainsKey(key, ch)

        member x.IsKnownSize = true
        member x.IsKnownCount = true
        
        member x.Count: Local<int64> = 
            source <!- GetCount

        member x.Size : Local<int64> =
            source <!- GetCount
        
        member x.Dispose(): Local<unit> = local.Zero ()
        
        member x.Id: string = id
        
        member x.Remove(key: string): Local<bool> = 
            source <!- fun ch -> Remove(key, ch)
        
        member x.ToEnumerable() = local {
            let! pairs = source <!- ToArray
            return pairs :> seq<_>
        }
        
        member x.TryAdd(key: string, value: 'T): Local<bool> = 
            source <!- fun ch -> Add(key, value, false, ch)
        
        member x.TryFind(key: string): Local<'T option> = 
            source <!- fun ch -> TryFind(key, ch)

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

//type ActorDictionaryProvider (state : RuntimeState) =
//    interface ICloudDictionaryProvider with
//        member __.IsSupportedValue _ = true
//        member __.Create<'T> () = async {
//            let! dict = state.ResourceFactory.RequestDictionary()
//            return dict :> ICloudDictionary<'T>
//        }