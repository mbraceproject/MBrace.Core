namespace MBrace.Core.Internals.InMemoryRuntime

open System
open System.Collections.Generic
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

[<AutoSerializable(false)>]
type private InMemoryAtom<'T> (initial : 'T) =
    let id = mkUUID()
    let container = ref (Some initial)

    let rec swap (f : 'T -> 'T) = 
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | cv ->
            let result = Interlocked.CompareExchange<'T option>(container, Option.map f cv, cv)
            if obj.ReferenceEquals(result, cv) then ()
            else Thread.SpinWait 20; swap f

    let force (t : 'T) =
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | _ -> container := Some t

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = local { return Option.get container.Value }
        member __.Update(updater, _) = local { return swap updater }
        member __.Force(value) = local { return force value }
        member __.Dispose () = local { return container := None }

[<Sealed; AutoSerializable(false)>]
type InMemoryAtomProvider () =
    let id = mkUUID()

    static member CreateConfiguration () : CloudAtomConfiguration =
        {
            AtomProvider = new InMemoryAtomProvider() :> ICloudAtomProvider
            DefaultContainer = ""
        }

    interface ICloudAtomProvider with
        member __.Name = "InMemoryAtomProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = mkUUID()
        member __.IsSupportedValue _ = true
        member __.CreateAtom<'T>(_, init : 'T) = async { return new InMemoryAtom<'T>(init) :> _ }
        member __.DisposeContainer _ = raise <| new NotImplementedException()

/// Defines an in-memory channel factory using mailbox processor
[<Sealed; AutoSerializable(false)>]
type InMemoryChannelProvider () =
    let id = mkUUID()

    static member CreateConfiguration () : CloudChannelConfiguration =
        {
            ChannelProvider = new InMemoryChannelProvider() :> ICloudChannelProvider
            DefaultContainer = ""
        }

    interface ICloudChannelProvider with
        member __.Name = "InMemoryChannelProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = mkUUID()

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| mkUUID()
            let mbox = Microsoft.FSharp.Control.MailboxProcessor<'T>.Start(fun _ -> async.Zero())
            let sender =
                {
                    new ISendPort<'T> with
                        member __.Id = id
                        member __.Send(msg : 'T) = local { return mbox.Post msg }
                }

            let receiver =
                {
                    new IReceivePort<'T> with
                        member __.Id = id
                        member __.Receive(?timeout : int) = local { return! Cloud.OfAsync <| mbox.Receive(?timeout = timeout) }
                        member __.Dispose() = raise <| new NotSupportedException()
                }

            return sender, receiver
        }

        member __.DisposeContainer _ = async.Zero()

/// Defines an in-memory dictionary factory using ConcurrentDictionary
[<Sealed; AutoSerializable(false)>]
type InMemoryDictionaryProvider() =
    interface ICloudDictionaryProvider with
        member s.IsSupportedValue _ = true
        member s.Create<'T> () = async {
            let id = mkUUID()
            let dict = new System.Collections.Concurrent.ConcurrentDictionary<string, 'T> ()
            return {
                new ICloudDictionary<'T> with
                    member x.Add(key : string, value : 'T) : Local<unit> =
                        local { return dict.[key] <- value }

                    member x.TryAdd(key: string, value: 'T): Local<bool> = 
                        local { return dict.TryAdd(key, value) }
                    
                    member x.AddOrUpdate(key: string, updater: 'T option -> 'T): Local<'T> = 
                        local { return dict.AddOrUpdate(key, updater None, fun _ curr -> updater (Some curr))}
                    
                    member x.ContainsKey(key: string): Local<bool> = 
                        local { return dict.ContainsKey key }
                    
                    member x.Count: Local<int64> = 
                        local { return int64 dict.Count }

                    member x.Size: Local<int64> = 
                        local { return int64 dict.Count }
                    
                    member x.Dispose(): Local<unit> = local.Zero()
                    
                    // capture provider in closure it avoid it being serialized
                    member x.Id: string = let _ = s.GetHashCode() in id
                    
                    member x.Remove(key: string): Local<bool> = 
                        local { return dict.TryRemove key |> fst }
                    
                    member x.ToEnumerable(): Local<seq<KeyValuePair<string, 'T>>> = 
                        local { return dict :> _ }
                    
                    member x.TryFind(key: string): Local<'T option> = 
                        local { return let ok,v = dict.TryGetValue key in if ok then Some v else None }
                    
            } }