namespace MBrace.Thespian

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

type private ChannelMsg<'T> =
    | Send of 'T
    | Receive of IReplyChannel<'T>

type Channel<'T> private (id : string, source : ActorRef<ChannelMsg<'T>>) =

    interface IReceivePort<'T> with
        member __.Id = id
        member __.Receive(?timeout : int) = async { return! source.PostWithReply(Receive, ?timeout = timeout) }
        member __.Dispose () = local.Zero()

    interface ISendPort<'T> with
        member __.Id = id
        member __.Send(msg : 'T) = async { return! source.AsyncPost(Send msg) }

    /// Initializes a new distributed queue instance.
    static member Init(id : string) =
        let self = ref Unchecked.defaultof<ActorRef<ChannelMsg<'T>>>
        let behaviour (messages : ImmutableQueue<'T>, receivers : ImmutableQueue<IReplyChannel<'T>>) msg = async {
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
                        self.Value <-- Send t
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

        self :=
            Actor.Stateful (ImmutableQueue.Empty, ImmutableQueue.Empty) behaviour
            |> Actor.Publish
            |> Actor.ref

        new Channel<'T>(id, self.Value)

type ActorChannelProvider (state : ResourceFactory) =
    let id = mkUUID()
    interface ICloudChannelProvider with
        member __.Name = "ActorChannel"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! ch = state.RequestResource(fun () -> Channel<'T>.Init(id))
            return ch :> ISendPort<'T>, ch :> IReceivePort<'T>
        }

        member __.DisposeContainer _ = async.Zero()