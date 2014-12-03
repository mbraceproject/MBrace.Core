namespace Nessos.MBrace

open Nessos.MBrace.Continuation

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