namespace MBrace.Core

/// Sending side of a distributed channel
type ISendPort<'T> =

    /// Channel identifier
    abstract Id : string

    /// <summary>
    ///     Sends a message over the channel
    /// </summary>
    /// <param name="message">Message to send.</param>
    abstract Send : message:'T -> Local<unit>

/// Receiving side of a distributed channel
type IReceivePort<'T> =
    inherit ICloudDisposable

    /// Channel identifier
    abstract Id : string

    /// <summary>
    ///     Asynchronously awaits a message from the channel.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds.</param>
    abstract Receive : ?timeout:int -> Local<'T>

namespace MBrace.Store

open MBrace.Core

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
type CloudChannelConfiguration =
    {
        /// Atom provider instance
        ChannelProvider : ICloudChannelProvider
        /// Default container for instance in current execution context.
        DefaultContainer : string
    }
with  
    /// <summary>
    ///     Creates a channel configuration instance using provided components.
    /// </summary>
    /// <param name="channelProvider">Channel provider instance.</param>
    /// <param name="defaultContainer">Default container for current process. Defaults to auto generated.</param>
    static member Create(channelProvider : ICloudChannelProvider, ?defaultContainer : string) =
        {
            ChannelProvider = channelProvider
            DefaultContainer = match defaultContainer with Some c -> c | None -> channelProvider.CreateUniqueContainerName()
        }

namespace MBrace.Core

open MBrace.Continuation
open MBrace.Store

#nowarn "444"

/// Channel methods for MBrace
type CloudChannel =

    /// <summary>
    ///     Creates a new channel instance.
    /// </summary>
    /// <param name="container">Container to channel. Defaults to process default.</param>
    static member New<'T>(?container : string) = local {
        let! config = Cloud.GetResource<CloudChannelConfiguration> ()
        let container = defaultArg container config.DefaultContainer
        return! ofAsync <| config.ChannelProvider.CreateChannel<'T> (container)
    }

    /// <summary>
    ///     Send message to the channel.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="channel">Target channel.</param>
    static member Send<'T> (channel : ISendPort<'T>, message : 'T) = local {
        return! channel.Send message
    }

    /// <summary>
    ///     Receive message from channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    static member Receive<'T> (channel : IReceivePort<'T>, ?timeout : int) = local {
        return! channel.Receive (?timeout = timeout)
    }

    /// <summary>
    ///     Deletes cloud channel instance.
    /// </summary>
    /// <param name="channel">Channel to be disposed.</param>
    static member Delete(channel : IReceivePort<'T>) : Local<unit> = 
        local { return! dispose channel }

    /// <summary>
    ///     Deletes container and all its contained channels.
    /// </summary>
    /// <param name="container"></param>
    static member DeleteContainer (container : string) = local {
        let! config = Cloud.GetResource<CloudChannelConfiguration> ()
        return! ofAsync <| config.ChannelProvider.DisposeContainer container
    }

    /// Generates a unique container name.
    static member CreateContainerName() = local {
        let! config = Cloud.GetResource<CloudChannelConfiguration> ()
        return config.ChannelProvider.CreateUniqueContainerName()
    }