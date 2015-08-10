namespace MBrace.Runtime.Vagabond

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
open MBrace.Runtime.Vagabond

[<AutoOpen>]
module FsPicklerExtensions =

    [<AutoSerializable(false)>]
    type FsPicklerObjectSizeCounter<'T> internal (pickler : Pickler<'T>, counter : ObjectSizeCounter) =
        let pickler = Some pickler // avoid optional allocation on every append by preallocating here

        member x.Append(graph: 'T): unit = counter.Append(graph, ?pickler = pickler)
        member x.TotalBytes: int64 = counter.Count
        member x.TotalObjects: int64 = counter.ObjectCount

        interface IObjectSizeCounter<'T> with
            member x.Append(graph: 'T): unit = counter.Append(graph, ?pickler = pickler)
            member x.TotalBytes: int64 = counter.Count
            member x.TotalObjects: int64 = counter.ObjectCount
            member x.Dispose () = ()

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
            new FsPicklerObjectSizeCounter<'T>(pickler, counter)

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



[<AbstractClass; AutoSerializable(true)>]
type FsPicklerStoreSerializer () as self =
    // force exception in case of Vagrant instance not initialized
    static do VagabondRegistry.Instance |> ignore

    // serializer instance registry for local AppDomain
    static let localInstances = new ConcurrentDictionary<string, FsPicklerSerializer> ()

    [<NonSerialized>]
    let mutable localInstance : FsPicklerSerializer option = None

    let getLocalInstance () =
        match localInstance with
        | Some instance -> instance
        | None ->
            // local instance not assigned, look up from registry
            let instance = localInstances.GetOrAdd(self.Id, ignore >> self.CreateLocalSerializerInstance)
            localInstance <- Some instance
            instance

    member __.Pickler = getLocalInstance()

    abstract Id : string
    abstract CreateLocalSerializerInstance : unit -> FsPicklerSerializer

    interface ISerializer with
        member __.Id = __.Id
        member __.Serialize (target : Stream, value : 'T, leaveOpen : bool) = getLocalInstance().Serialize(target, value, leaveOpen = leaveOpen)
        member __.Deserialize<'T>(stream, leaveOpen) = getLocalInstance().Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member __.SeqSerialize(stream, values : 'T seq, leaveOpen) = getLocalInstance().SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member __.SeqDeserialize<'T>(stream, leaveOpen) = getLocalInstance().DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)
        member __.ComputeObjectSize<'T>(graph:'T) = getLocalInstance().ComputeSize graph
        member __.CreateObjectSizeCounter<'T> () = FsPickler.CreateTypedObjectSizeCounter<'T> (serializer = getLocalInstance()) :> _

[<AutoSerializable(true)>]
type FsPicklerBinaryStoreSerializer () =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler binary serializer"
    override __.CreateLocalSerializerInstance () = VagabondRegistry.Instance.Serializer :> _


[<AutoSerializable(true)>]
type FsPicklerXmlStoreSerializer (?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler xml serializer"
    override __.CreateLocalSerializerInstance () = FsPickler.CreateXmlSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent) :> _

[<AutoSerializable(true)>]
type FsPicklerJsonStoreSerializer (?omitHeader : bool, ?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler json serializer"
    override __.CreateLocalSerializerInstance () = FsPickler.CreateJsonSerializer(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent, ?omitHeader = omitHeader) :> _