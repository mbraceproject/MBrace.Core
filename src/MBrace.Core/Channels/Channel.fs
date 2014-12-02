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

namespace Nessos.MBrace.Continuation

open Nessos.MBrace

/// Defines a factory for distributed channels
type ICloudChannelProvider =
    /// Creates a new channel instance for given type
    abstract CreateChannel<'T> : unit -> Async<ISendPort<'T> * IReceivePort<'T>>

/// Defines an in-memory channel factory using F# MailboxProcessor
type MailboxChannelProvider () =
    interface ICloudChannelProvider with
        member __.CreateChannel<'T> () = async {
            let mbox = MailboxProcessor<'T>.Start(fun _ -> async.Zero())
            let sender =
                {
                    new ISendPort<'T> with
                        member __.Send(msg : 'T) = async { return mbox.Post msg }
                }

            let receiver =
                {
                    new IReceivePort<'T> with
                        member __.Receive(?timeout : int) = mbox.Receive(?timeout = timeout)
                        member __.Dispose() = async.Zero()
                }

            return sender, receiver
        }