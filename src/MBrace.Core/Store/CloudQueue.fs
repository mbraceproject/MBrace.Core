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
    ///     Asynchronously dequeues a batch of messages from the queue.
    ///     Will dequeue up to the given maximum number of items, depending on current availability.
    /// </summary>
    /// <param name="maxItems">Maximum number of items to dequeue.</param>
    abstract DequeueBatch : maxItems:int -> Async<'T []>

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

    /// Generates a unique, random queue name.
    abstract GetRandomQueueName : unit -> string

    /// <summary>
    ///     Creates a new queue instance of given type.
    /// </summary>
    /// <param name="queueId">Unique queue identifier.</param>
    abstract CreateQueue<'T> : queueId:string -> Async<CloudQueue<'T>>

    /// <summary>
    ///     Attempt to recover an existing queue instance of specified id.
    /// </summary>
    /// <param name="queueId">Queue identifier.</param>
    abstract GetQueueById<'T> : queueId:string -> Async<CloudQueue<'T>>


namespace MBrace.Core

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Queue methods for MBrace
type CloudQueue =

    /// <summary>
    ///     Creates a new queue instance.
    /// </summary>
    /// <param name="queueId">Unique queue identifier. Defaults to randomly generated queue name.</param>
    static member New<'T>(?queueId : string) = local {
        let! provider = Cloud.GetResource<ICloudQueueProvider> ()
        let queueId = match queueId with Some qi -> qi | None -> provider.GetRandomQueueName()
        return! provider.CreateQueue<'T> queueId
    }

    /// <summary>
    ///     Attempt to recover an existing of given type and identifier.
    /// </summary>
    /// <param name="queueId">Unique queue identifier.</param>
    static member GetById<'T>(queueId : string) = local {
        let! provider = Cloud.GetResource<ICloudQueueProvider> ()
        return! provider.GetQueueById<'T> queueId
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
    ///     Dequeues a batch of messages from the queue.
    ///     Will dequeue up to the given maximum number of items, depending on current availability.
    /// </summary>
    /// <param name="queue">Source queue.</param>
    /// <param name="maxItems">Maximum number of items to dequeue.</param>
    static member DequeueBatch<'T>(queue : CloudQueue<'T>, maxItems : int) = local {
        return! queue.DequeueBatch(maxItems)
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