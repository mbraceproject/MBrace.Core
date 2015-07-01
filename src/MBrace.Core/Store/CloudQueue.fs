namespace MBrace.Core

open MBrace.Core

/// Distributed Queue abstraction
type ICloudQueue<'T> =
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

    /// Create a uniquely specified container name.
    abstract CreateUniqueContainerName : unit -> string

    /// <summary>
    ///     Creates a new queue instance of given type.
    /// </summary>
    /// <param name="container">Container for queue.</param>
    abstract CreateQueue<'T> : container:string -> Async<ICloudQueue<'T>>

    /// <summary>
    ///     Disposes all queues in provided container.
    /// </summary>
    /// <param name="container">Queue container.</param>
    abstract DisposeContainer : container:string -> Async<unit>

/// Queue configuration passed to the continuation execution context
[<NoEquality; NoComparison>]
type CloudQueueConfiguration =
    {
        /// Atom provider instance
        QueueProvider : ICloudQueueProvider
        /// Default container for instance in current execution context.
        DefaultContainer : string
    }
with  
    /// <summary>
    ///     Creates a queue configuration instance using provided components.
    /// </summary>
    /// <param name="queueProvider">Queue provider instance.</param>
    /// <param name="defaultContainer">Default container for current process. Defaults to auto generated.</param>
    static member Create(queueProvider : ICloudQueueProvider, ?defaultContainer : string) =
        {
            QueueProvider = queueProvider
            DefaultContainer = match defaultContainer with Some c -> c | None -> queueProvider.CreateUniqueContainerName()
        }

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
        let! config = Cloud.GetResource<CloudQueueConfiguration> ()
        let container = defaultArg container config.DefaultContainer
        return! config.QueueProvider.CreateQueue<'T> (container)
    }

    /// <summary>
    ///     Add message to the queue.
    /// </summary>
    /// <param name="message">Message to send.</param>
    /// <param name="queue">Target queue.</param>
    static member Enqueue<'T> (queue : ICloudQueue<'T>, message : 'T) = local {
        return! queue.Enqueue message
    }

    /// <summary>
    ///     Add batch of messages to the queue.
    /// </summary>
    /// <param name="messages">Message to be enqueued.</param>
    /// <param name="queue">Target queue.</param>
    static member EnqueueBatch<'T> (queue : ICloudQueue<'T>, messages : seq<'T>) = local {
        return! queue.EnqueueBatch messages
    }

    /// <summary>
    ///     Receive message from queue.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="timeout">Timeout in milliseconds.</param>
    static member Dequeue<'T> (queue : ICloudQueue<'T>, ?timeout : int) = local {
        return! queue.Dequeue (?timeout = timeout)
    }

    /// <summary>
    ///     Asynchronously attempts to dequeue a message from the queue.
    ///     Returns None instantly if no message is currently available.
    /// </summary>
    /// <param name="queue"></param>
    static member TryDequeue<'T> (queue : ICloudQueue<'T>) = local {
        return! queue.TryDequeue()
    }

    /// <summary>
    ///     Deletes cloud queue instance.
    /// </summary>
    /// <param name="queue">Queue to be disposed.</param>
    static member Delete(queue : ICloudQueue<'T>) : Local<unit> = 
        local { return! dispose queue }

    /// <summary>
    ///     Deletes container and all its contained queues.
    /// </summary>
    /// <param name="container"></param>
    static member DeleteContainer (container : string) = local {
        let! config = Cloud.GetResource<CloudQueueConfiguration> ()
        return! config.QueueProvider.DisposeContainer container
    }

    /// Generates a unique container name.
    static member CreateContainerName() = local {
        let! config = Cloud.GetResource<CloudQueueConfiguration> ()
        return config.QueueProvider.CreateUniqueContainerName()
    }