namespace Nessos.MBrace

/// Sending side of a distributed channel
type ISendPort<'T> =
    /// <summary>
    ///     Sends a message over the channel
    /// </summary>
    /// <param name="message">Message to send.</param>
    abstract Send : message:'T -> Async<unit>

/// Receiving side of a distributed channel
type IReceivePort<'T> =
    inherit ICloudDisposable
    /// <summary>
    ///     Asynchronously awaits a message from the channel.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds.</param>
    abstract Receive : ?timeout:int -> Async<'T>

namespace Nessos.MBrace.Store

open Nessos.MBrace

/// Defines a factory for distributed channels
type ICloudChannelProvider =

    /// unique cloud file store identifier
    abstract Id : string

    /// <summary>
    ///     Creates a new channel instance for given type
    /// </summary>
    /// <param name="container">Container id for current instance.</param>
    abstract CreateChannel<'T> : unit -> Async<ISendPort<'T> * IReceivePort<'T>>

namespace Nessos.MBrace

open Nessos.MBrace.Continuation
open Nessos.MBrace.Store

#nowarn "444"

/// Channel methods for MBrace
type CloudChannel =

    /// Creates a new channel instance.
    static member New<'T>() = cloud {
        let! provider = Cloud.GetResource<ICloudChannelProvider> ()
        return! Cloud.OfAsync <| provider.CreateChannel<'T> ()
    }

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="channel">Target channel.</param>
    static member Send<'T> (message : 'T) (channel : ISendPort<'T>) = cloud {
        return! Cloud.OfAsync <| channel.Send message
    }

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    static member Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) = cloud {
        return! Cloud.OfAsync <| channel.Receive (?timeout = timeout)
    }

///// Defines an in-memory channel factory using F# MailboxProcessor
//type MailboxChannelProvider () =
//    interface ICloudChannelProvider with
//        member __.CreateChannel<'T> () = async {
//            let mbox = MailboxProcessor<'T>.Start(fun _ -> async.Zero())
//            let sender =
//                {
//                    new ISendPort<'T> with
//                        member __.Send(msg : 'T) = async { return mbox.Post msg }
//                }
//
//            let receiver =
//                {
//                    new IReceivePort<'T> with
//                        member __.Receive(?timeout : int) = mbox.Receive(?timeout = timeout)
//                        member __.Dispose() = async.Zero()
//                }
//
//            return sender, receiver
//        }