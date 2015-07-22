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
            let counter = match serializer with Some s -> s.CreateObjectSizeCounter() | None -> FsPickler.CreateSizeCounter()
            new FsPicklerObjectSizeCounter<'T>(pickler, counter)

        /// <summary>
        ///     Asynchronously partitions a sequence into chunks, separated by a chunk threshold size in bytes.
        /// </summary>
        /// <param name="sequence">Input sequence to be partitioned.</param>
        /// <param name="partitionThreshold">Partition threshold in bytes.</param>
        /// <param name="chunkContinuation">Asynchronous chunk continuation.</param>
        static member PartitionSequenceBySize(sequence : seq<'T>, partitionThreshold : int64, chunkContinuation : 'T[] -> Async<'R>, ?pickler : Pickler<'T>, ?serializer : FsPicklerSerializer) : Async<'R []> = async {
            if partitionThreshold <= 0L then invalidArg "partitionThreshold" "must be positive value."
            let pickler = match pickler with None -> FsPickler.GeneratePickler<'T> () | Some p -> p
            let sizeCounter = match serializer with Some s -> s.CreateObjectSizeCounter() | None -> FsPickler.CreateSizeCounter()
            use chunkEnumerator = new ChunkEnumerator<'T>(sequence, pickler, sizeCounter, partitionThreshold)
            // rather than creating all chunks and mapping everything to Async.Parallel,
            // this strategy passes the chunk to its continuation as it is being created in a separate thread.
            let taskAggregator = new ResizeArray<Task<'R>> ()
            do while chunkEnumerator.MoveNext() do
                let task = Async.StartAsTask(chunkContinuation chunkEnumerator.Current)
                taskAggregator.Add task

            let all = taskAggregator.ToArray()
            if Array.isEmpty all then return [||] else
            let t = Task.Factory.ContinueWhenAll(all, ignore)
            let! _ = Async.AwaitTask(t) |> Async.Catch
            return all |> Array.map (fun t -> t.GetResult())
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
    override __.CreateLocalSerializerInstance () = FsPickler.CreateXml(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent) :> _

[<AutoSerializable(true)>]
type FsPicklerJsonStoreSerializer (?omitHeader : bool, ?indent : bool) =
    inherit FsPicklerStoreSerializer()

    override __.Id = "FsPickler json serializer"
    override __.CreateLocalSerializerInstance () = FsPickler.CreateJson(typeConverter = VagabondRegistry.Instance.TypeConverter, ?indent = indent, ?omitHeader = omitHeader) :> _