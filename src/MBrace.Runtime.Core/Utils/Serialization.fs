namespace MBrace.Runtime.Utils

open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime

[<AutoOpen>]
module FsPicklerExtensions =

    [<AutoSerializable(false)>]
    type TypedObjectSizeCounter<'T> internal (pickler : Pickler<'T>, counter : ObjectSizeCounter) =
        let pickler = Some pickler // avoid optional allocation on every append by preallocating here

        member x.Append(graph: 'T): unit = counter.Append(graph, ?pickler = pickler)
        member x.TotalBytes: int64 = counter.Count
        member x.TotalObjects: int64 = counter.ObjectCount

    [<AutoSerializable(false)>]
    type private ChunkEnumerator<'T> (sequence : seq<'T>, pickler : Pickler<'T>, counter : ObjectSizeCounter, threshold : int64) =
        let pickler = Some pickler // avoid optional allocation on every append by preallocating here
        let enumerator = sequence.GetEnumerator()
        let mutable chunk = Unchecked.defaultof<'T []>
        let mutable hasNext = true

        member x.Current: 'T [] = chunk
        member x.MoveNext(): bool =
            if not hasNext then false else
            let chunkBuilder = new ResizeArray<'T> ()
            while counter.Count <= threshold && (enumerator.MoveNext() || (hasNext <- false ; false)) do
                let t = enumerator.Current
                chunkBuilder.Add t
                counter.Append(t, ?pickler = pickler)

            if hasNext || chunkBuilder.Count > 0 then
                counter.Reset()
                chunk <- chunkBuilder.ToArray()
                true
            else
                false
        
        interface IDisposable with
            member x.Dispose(): unit = enumerator.Dispose()

    type Nessos.FsPickler.FsPickler with

        /// <summary>
        ///     Creates a typed object size counter instance.
        /// </summary>
        /// <param name="pickler">Custom pickler to use</param>
        static member CreateTypedObjectSizeCounter<'T> (?pickler : Pickler<'T>, ?serializer : FsPicklerSerializer) =
            let pickler = match pickler with None -> FsPickler.GeneratePickler<'T> () | Some p -> p
            let counter = match serializer with Some s -> s.CreateObjectSizeCounter() | None -> FsPickler.CreateObjectSizeCounter()
            new TypedObjectSizeCounter<'T>(pickler, counter)

        /// <summary>
        ///     Asynchronously partitions a sequence into chunks, separated by a chunk threshold size in bytes.
        /// </summary>
        /// <param name="sequence">Input sequence to be partitioned.</param>
        /// <param name="partitionThreshold">Partition threshold in bytes.</param>
        /// <param name="chunkContinuation">Asynchronous chunk continuation.</param>
        /// <param name="pickler">Pickler used for size computation.</param>
        /// <param name="serializer">Serializer used for size counting.</param>
        /// <param name="maxConcurrentContinuations">Maximum number of concurrent continuations allowed while evaluating sequence. Defaults to infinte.</param>
        static member PartitionSequenceBySize(sequence : seq<'T>, partitionThreshold : int64, chunkContinuation : 'T[] -> Async<'R>, 
                                                ?pickler : Pickler<'T>, ?serializer : FsPicklerSerializer, ?maxConcurrentContinuations : int) : Async<'R []> = async {
            if partitionThreshold <= 0L then invalidArg "partitionThreshold" "must be positive value."
            let pickler = match pickler with None -> FsPickler.GeneratePickler<'T> () | Some p -> p
            let sizeCounter = match serializer with Some s -> s.CreateObjectSizeCounter() | None -> FsPickler.CreateObjectSizeCounter()
            use chunkEnumerator = new ChunkEnumerator<'T>(sequence, pickler, sizeCounter, partitionThreshold)
            let taskQueue = new Queue<Task<'R>> ()
            let results = new ResizeArray<'R> ()
            while chunkEnumerator.MoveNext() do
                if maxConcurrentContinuations |> Option.exists (fun cc -> cc < taskQueue.Count) then
                    // concurrent continuation limit has been reached,
                    // asynchronously wait until first task in queue has completed
                    let t = taskQueue.Dequeue()
                    let! result = t.AwaitResultAsync() // might raise excepton, but we are ok with this
                    results.Add result

                // no restrictions, apply current chunk to continuation 
                // and append to task queue
                let task = Async.StartAsTask(chunkContinuation chunkEnumerator.Current)
                taskQueue.Enqueue task

            for t in taskQueue do
                let! result = t.AwaitResultAsync()
                results.Add result

            return results.ToArray()
        }