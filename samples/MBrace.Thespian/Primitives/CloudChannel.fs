namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

[<AutoOpen>]
module private ActorChannel =

    type ChannelMsg =
        | Send of byte[]
        | Receive of IReplyChannel<byte[]>

    /// Queue actor internal state : Enqueued messages * Subscribed receivers
    type ChannelState = ImmutableQueue<byte []> * ImmutableQueue<IReplyChannel<byte []>>

    /// <summary>
    ///     Initializes a channel actor instance in the local process.
    /// </summary>
    let init () : ActorRef<ChannelMsg> =
        let behaviour (self : Actor<ChannelMsg>) ((messages, receivers) : ChannelState) (msg : ChannelMsg) = async {
            match msg with
            | Send t ->
                match receivers.TryDequeue () with
                | Some(rc, receivers') ->
                    // receiving side may have timed out a long time ago, protect
                    try 
                        do! rc.Reply t
                        return (messages, receivers')
                    with e ->
                        // reply failed, re-enqueue
                        self.Ref <-- Send t
                        return (messages, receivers')

                | None ->
                    return (messages.Enqueue t, receivers)

            | Receive rc ->
                match messages.TryDequeue () with
                | Some(t, senders') ->
                    do! rc.Reply t
                    return (senders', receivers)
                | None ->
                    return messages, receivers.Enqueue rc
        }

        Actor.SelfStateful (ImmutableQueue.Empty, ImmutableQueue.Empty) behaviour
        |> Actor.Publish
        |> Actor.ref

    /// Actor CloudChannel implementation
    type ActorChannel<'T> internal (id : string, source : ActorRef<ChannelMsg>) =

        interface IReceivePort<'T> with
            member __.Id = id
            member __.Receive(?timeout : int) = async { 
                let! result = source.PostWithReply(Receive, ?timeout = timeout) 
                return Config.Serializer.UnPickle<'T> result
            }

            member __.Dispose () = local.Zero()

        interface ISendPort<'T> with
            member __.Id = id
            member __.Send(msg : 'T) = async { 
                let msgP = Config.Serializer.Pickle msg
                return! source.AsyncPost(Send msgP) 
            }

/// Defines a distributed cloud channel factory
type ActorChannelProvider (factory : ResourceFactory) =
    let id = mkUUID()
    interface ICloudChannelProvider with
        member __.Name = "ActorChannel"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! actor = factory.RequestResource(fun () -> ActorChannel.init())
            let ach = new ActorChannel<'T>(id, actor)
            return ach :> ISendPort<'T>, ach :> IReceivePort<'T>
        }

        member __.DisposeContainer _ = async.Zero()