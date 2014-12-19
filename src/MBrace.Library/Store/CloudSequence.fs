namespace Nessos.MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Continuation

type private CloudSequenceCache =

    static member inline ContainsKey<'T>(uuid) =
        match InMemoryCacheRegistry.InstalledCache with
        | Some c -> c.ContainsKey uuid
        | _ -> false
    
    static member inline Add<'T>(uuid, values : unit -> 'T []) =
        match InMemoryCacheRegistry.InstalledCache with
        | Some c when not <| c.ContainsKey uuid -> c.TryAdd(uuid, values ()) |> ignore
        | _ -> ()

    static member inline TryFind<'T>(uuid) =
        match InMemoryCacheRegistry.InstalledCache with
        | None -> None
        | Some c -> c.TryFind<'T []>(uuid)

/// <summary>
///     Ordered, immutable collection of values persisted in a single FileStore entity.
/// </summary>
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudSequence<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "UUID")>]
    val mutable private uuid : string
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "Count")>]
    val mutable private count : int option
    [<DataMember(Name = "FileStore")>]
    val mutable private fileStore : ICloudFileStore
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer

    private new (path, count, fileStore, serializer) = 
        let uuid = Guid.NewGuid().ToString()
        { uuid = uuid ; path = path ; count = count ; fileStore = fileStore ; serializer = serializer }

    member private c.GetSequenceFromStore() = async {
        // header and payload found on same file
        let! stream = c.fileStore.BeginRead(c.path)
        return c.serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
    }

    /// Read elements as lazy sequence
    member c.GetSequenceAsync () = async {
        match CloudSequenceCache.TryFind c.uuid with
        | None -> return! c.GetSequenceFromStore ()
        | Some ts -> return ts :> seq<'T>
    }

    /// Saves Cloud sequence contents to in-memory array
    member c.ToArray () : 'T [] = 
        match CloudSequenceCache.TryFind c.uuid with
        | None -> c.GetSequenceFromStore () |> Async.RunSync |> Seq.toArray
        | Some ts -> ts

    // Cache contents to local memory
    member c.Cache () = 
        let mkArray () = c.GetSequenceFromStore() |> Async.RunSync |> Seq.toArray
        CloudSequenceCache.Add(c.uuid, mkArray)

    /// Indicates if array is cached in local context
    member c.IsCachedLocally = CloudSequenceCache.ContainsKey c.uuid

    /// Path to Cloud sequence in store
    member c.Path = c.path

//    member c.Item
//        with get i =
//            match CloudSequenceCache.TryFind c.uuid with
//            | None -> c.GetSequenceFromStore () |> Async.RunSync |> Seq.nth i
//            | Some ts -> ts.[i]

    /// Cloud sequence element count
    member c.Count = 
        match c.count with
        | Some l -> l
        | None ->
            // this is a potentially costly operation
            let l = c.GetSequenceAsync() |> Async.RunSync |> Seq.length
            c.count <- Some l
            l

    /// Underlying sequence size in bytes
    member c.Size = c.fileStore.GetFileSize c.path |> Async.RunSync

    interface ICloudStorageEntity with
        member c.Type = sprintf "CloudSequence:%O" typeof<'T>
        member c.Id = c.path

    interface ICloudDisposable with
        member c.Dispose () = c.fileStore.DeleteFile c.path

    interface IEnumerable<'T> with
        member c.GetEnumerator() = (c.GetSequenceAsync() |> Async.RunSync :> IEnumerable).GetEnumerator()
        member c.GetEnumerator() = (c.GetSequenceAsync() |> Async.RunSync).GetEnumerator()

    interface IReadOnlyCollection<'T> with
        member c.Count = c.Count

    override c.ToString() = sprintf "CloudSequence[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()

    /// <summary>
    ///     Creates a new Cloud sequence with given values in provided file store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">Containing directory in file store.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member Create (values : seq<'T>, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) = async {
        let path = fileStore.GetRandomFilePath directory
        use! stream = fileStore.BeginWrite path
        let length = serializer.SeqSerialize<'T>(stream, values, leaveOpen = false)
        return new CloudSequence<'T>(path, Some length, fileStore, serializer)
    }

    /// <summary>
    ///     Writes sequence of values into a collection of Cloud sequences partitioned by serialization size.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum partition size in bytes.</param>
    /// <param name="directory">Containing directory in file store.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    static member CreatePartitioned(values : seq<'T>, maxPartitionSize : int64, directory : string, fileStore : ICloudFileStore, serializer : ISerializer) : Async<CloudSequence<'T> []> = 
        async {
            if maxPartitionSize <= 0L then return invalidArg "maxPartitionSize" "Must be greater that 0."

            let seqs = new ResizeArray<CloudSequence<'T>>()
            let currentStream = ref Unchecked.defaultof<Stream>
            let splitNext () = currentStream.Value.Position >= maxPartitionSize
            let partitionedValues = PartitionedEnumerable.ofSeq splitNext values
            for partition in partitionedValues do
                let path = fileStore.GetRandomFilePath directory
                use! stream = fileStore.BeginWrite path
                currentStream := stream
                let length = serializer.SeqSerialize<'T>(stream, partition, leaveOpen = false)
                let seq = new CloudSequence<'T>(path, Some length, fileStore, serializer)
                seqs.Add seq

            return seqs.ToArray()
        }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="serializer">Serializer instance.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member Parse(path : string, fileStore : ICloudFileStore, serializer : ISerializer, ?force : bool) = async {
        let force = defaultArg force false
        let cseq = new CloudSequence<'T>(path, None, fileStore, serializer)
        if force then cseq.Count |> ignore
        return cseq
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="fileStore">File store instance.</param>
    /// <param name="deserializer">Deserializer instance.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member Parse(path : string, fileStore : ICloudFileStore, deserializer : Stream -> seq<'T>, ?force : bool) = async {
        let serializer =
            {
                new ISerializer with
                    member __.Id = "Deserializer Lambda"
                    member __.Serialize(_,_,_) = raise <| new NotSupportedException()
                    member __.Deserialize(_,_) = raise <| new NotSupportedException()
                    member __.SeqSerialize(_,_,_) = raise <| new NotSupportedException()
                    member __.SeqDeserialize<'a>(source,_) = deserializer source :> obj :?> seq<'a>
            }

        return! CloudSequence<'T>.Parse(path, fileStore, serializer, ?force = force)
    }

#nowarn "444"

type CloudSequence =

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member New(values : seq<'T>, ?directory, ?serializer) : Cloud<CloudSequence<'T>> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let dir = defaultArg directory config.DefaultDirectory
        return! Cloud.OfAsync <| CloudSequence<'T>.Create(values, dir, config.FileStore, serializer)
    }

    /// <summary>
    ///     Creates a collection of Cloud sequences partitioned by file size.
    /// </summary>
    /// <param name="values">Input sequence./param>
    /// <param name="maxPartitionSize">Maximum size in bytes per Cloud sequence partition.</param>
    /// <param name="directory"></param>
    /// <param name="serializer"></param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member NewPartitioned(values : seq<'T>, maxPartitionSize, ?directory, ?serializer) : Cloud<CloudSequence<'T> []> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let dir = defaultArg directory config.DefaultDirectory
        return! Cloud.OfAsync <| CloudSequence<'T>.CreatePartitioned(values, maxPartitionSize, dir, config.FileStore, serializer)
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member Parse<'T>(path : string, ?serializer, ?force) : Cloud<CloudSequence<'T>> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        return! Cloud.OfAsync <| CloudSequence<'T>.Parse(path, config.FileStore, serializer, ?force = force)
    }

    /// <summary>
    ///     Creates a CloudSequence from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromFile<'T>(path : string, deserializer : Stream -> seq<'T>, ?force) : Cloud<CloudSequence<'T>> = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! Cloud.OfAsync <| CloudSequence<'T>.Parse(path, config.FileStore, deserializer, ?force = force)
    }