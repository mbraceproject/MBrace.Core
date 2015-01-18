namespace MBrace

/// Sending side of a distributed channel
type ISendPort<'T> =

    /// Channel identifier
    abstract Id : string

    /// <summary>
    ///     Sends a message over the channel
    /// </summary>
    /// <param name="message">Message to send.</param>
    abstract Send : message:'T -> Async<unit>

/// Receiving side of a distributed channel
type IReceivePort<'T> =
    inherit ICloudDisposable

    /// Channel identifier
    abstract Id : string

    /// <summary>
    ///     Asynchronously awaits a message from the channel.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds.</param>
    abstract Receive : ?timeout:int -> Async<'T>

namespace MBrace.Store

open MBrace

/// Defines a factory for distributed channels
type ICloudChannelProvider =

    /// Implementation name
    abstract Name : string

    /// unique cloud channel source identifier
    abstract Id : string

    /// Create a uniquely specified container name.
    abstract CreateUniqueContainerName : unit -> string

    /// <summary>
    ///     Creates a new channel instance for given type.
    /// </summary>
    /// <param name="container">Container for channel.</param>
    abstract CreateChannel<'T> : container:string -> Async<ISendPort<'T> * IReceivePort<'T>>

    /// <summary>
    ///     Disposes all atoms in provided container
    /// </summary>
    /// <param name="container">Atom container.</param>
    abstract DisposeContainer : container:string -> Async<unit>

/// Channel configuration passed to the continuation execution context
type ChannelConfiguration =
    {
        /// Atom provider instance
        ChannelProvider : ICloudChannelProvider
        /// Default container for instance in current execution context.
        DefaultContainer : string
    }

namespace MBrace

open MBrace.Continuation
open MBrace.Store

#nowarn "444"

/// Channel methods for MBrace
type CloudChannel =

    /// Creates a new channel instance.
    static member New<'T>() = cloud {
        let! config = Cloud.GetResource<ChannelConfiguration> ()
        return! Cloud.OfAsync <| config.ChannelProvider.CreateChannel<'T> (config.DefaultContainer)
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