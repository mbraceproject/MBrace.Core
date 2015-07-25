namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils

[<AutoOpen>]
module private ActorQueue =

    type QueueMsg =
        | GetMessageCount of IReplyChannel<int>
        | Enqueue of byte[]
        | BatchEnqueue of byte [][]
        | TryDequeue of IReplyChannel<byte [] option>

    /// Queue actor internal state : Enqueued messages * Subscribed receivers
    type QueueState = ImmutableQueue<byte []>

    /// <summary>
    ///     Initializes a channel actor instance in the local process.
    /// </summary>
    let init () : ActorRef<QueueMsg> =
        let behaviour (state : QueueState) (msg : QueueMsg) = async {
            match msg with
            | GetMessageCount rc ->     
                do! rc.Reply state.Count
                return state

            | Enqueue t -> return state.Enqueue t
            | BatchEnqueue ts -> return state.EnqueueMultiple (Array.toList ts)
            | TryDequeue rc ->
                match state.TryDequeue () with
                | Some (t, state') ->
                    do! rc.Reply (Some t)
                    return state'
                | None ->
                    do! rc.Reply None
                    return state
        }

        Actor.Stateful ImmutableQueue.Empty behaviour
        |> Actor.Publish
        |> Actor.ref

    /// Actor CloudQueue implementation
    type ActorQueue<'T> internal (id : string, source : ActorRef<QueueMsg>) =

        interface CloudQueue<'T> with
            member __.Id = id

            member __.Count = async {
                let! count = source <!- GetMessageCount
                return int64 count
            }

            member __.Enqueue(msg : 'T) = async { 
                let msgP = Config.Serializer.Pickle msg
                return! source.AsyncPost(Enqueue msgP) 
            }

            member __.EnqueueBatch(messages : seq<'T>) = async {
                let msgs = messages |> Seq.map Config.Serializer.Pickle |> Seq.toArray
                return! source.AsyncPost(BatchEnqueue msgs)
            }

            member __.Dequeue(?timeout : int) = async { 
                let rec poll () = async {
                    let! result = source <!- TryDequeue
                    match result with
                    | Some t -> return Config.Serializer.UnPickle<'T> t
                    | None ->
                        do! Async.Sleep 100
                        return! poll()
                }

                match timeout with
                | None -> return! poll ()
                | Some t -> return! Async.WithTimeout(poll(), t)
            }

            member __.TryDequeue() = async {
                let! result = source <!- TryDequeue
                return result |> Option.map (fun p -> Config.Serializer.UnPickle<'T> p)
            }

            member __.Dispose () = async.Zero()

/// Defines a distributed cloud channel factory
type ActorQueueProvider (factory : ResourceFactory) =
    let id = mkUUID()
    interface ICloudQueueProvider with
        member __.Name = "ActorQueue"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateQueue<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! actor = factory.RequestResource(fun () -> ActorQueue.init())
            return new ActorQueue<'T>(id, actor) :> CloudQueue<'T>
        }

        member __.DisposeContainer _ = async.Zero()