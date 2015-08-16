namespace MBrace.Core

open MBrace.Core

/// Distributed Queue abstraction
type CloudQueue<'T> =
    inherit ICloudDisposable

    /// Queue identifier
    abstract Id : string

    /// Gets the current message count of the queue
    abstract Count : Async<int64>

    /// <summary>
    ///     Asynchronously enqueues a message to the start of the queue.
    /// </summary>
    /// <param name="message">Message to be queued.</param>
    abstract Enqueue : message:'T -> Async<unit>

    /// <summary>
    ///     Enqueues a batch of messages to the start of the queue.
    /// </summary>
    /// <param name="messages">Messages to be enqueued.</param>
    abstract EnqueueBatch : messages:seq<'T> -> Async<unit>

    /// <summary>
    ///     Asynchronously dequeues a message from the queue.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds. Defaults to no timeout.</param>
    abstract Dequeue : ?timeout:int -> Async<'T>

    /// <summary>
    ///     Asynchronously attempts to dequeue a message from the queue.
    ///     Returns None instantly if no message is currently available.
    /// </summary>
    abstract TryDequeue : unit -> Async<'T option>

namespace MBrace.Core.Internals

open MBrace.Core

/// Defines a factory for distributed queues
type ICloudQueueProvider =

    /// Implementation name
    abstract Name : string

    /// unique cloud queue source identifier
    abstract Id : string

    /// Gets the default container used by the queue provider
    abstract DefaultContainer : string

    /// <summary>
    ///     Creates a copy of the queue provider with updated default container.
    /// </summary>
    /// <param name="container">Container to be updated.</param>
    abstract WithDefaultContainer : container:string -> ICloudQueueProvider

    /// Create a uniquely specified container name.
    abstract CreateUniqueContainerName : unit -> string

    /// <summary>
    ///     Creates a new queue instance of given type.
    /// </summary>
    /// <param name="container">Container for queue.</param>
    abstract CreateQueue<'T> : container:string -> Async<CloudQueue<'T>>

    /// <summary>
    ///     Disposes all queues in provided container.
    /// </summary>
    /// <param name="container">Queue container.</param>
    abstract DisposeContainer : container:string -> Async<unit>

namespace MBrace.Core

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Queue methods for MBrace
type CloudQueue =

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="container">Container to queue. Defaults to process default.</param>
    static member New<'T>(?container : string) = local {
        let! provider = Cloud.GetResource<ICloudQueueProvider> ()
        let container = defaultArg container provider.DefaultContainer
        return! provider.CreateQueue<'T> (container)
    }

    /// <summary>
    ///     Add message to the queue.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="queue">Target queue.</param>
    static member Enqueue<'T> (queue : CloudQueue<'T>, message : 'T) = local {
        return! queue.Enqueue message
    }

    /// <summary>
    ///     Add batch of messages to the queue.
    /// </summary>
    /// <param name="messages">Message to be enqueued.</param>
    /// <param name="queue">Target queue.</param>
    static member EnqueueBatch<'T> (queue : CloudQueue<'T>, messages : seq<'T>) = local {
        return! queue.EnqueueBatch messages
    }

    /// <summary>
    ///     Receive message from queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    static member Dequeue<'T> (queue : CloudQueue<'T>, ?timeout : int) = local {
        return! queue.Dequeue (?timeout = timeout)
    }

    /// <summary>
    ///     Asynchronously attempts to dequeue a message from the queue.
    ///     Returns None instantly if no message is currently available.
    /// </summary>
    /// <param name="queue"></param>
    static member TryDequeue<'T> (queue : CloudQueue<'T>) = local {
        return! queue.TryDequeue()
    }

    /// <summary>
    ///     Deletes cloud queue instance.
    /// </summary>
    /// <param name="queue">Queue to be disposed.</param>
    static member Delete(queue : CloudQueue<'T>) : Local<unit> = 
        local { return! queue.Dispose() }

    /// <summary>
    ///     Deletes container and all its contained queues.
    /// </summary>
    /// <param name="container"></param>
    static member DeleteContainer (container : string) = local {
        let! provider = Cloud.GetResource<ICloudQueueProvider> ()
        return! provider.DisposeContainer container
    }

    /// Generates a unique container name.
    static member CreateContainerName() = local {
        let! provider = Cloud.GetResource<ICloudQueueProvider> ()
        return provider.CreateUniqueContainerName()
    }