namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Store
open MBrace.Continuation

#nowarn "444"

/// Partition a seq<'T> to seq<seq<'T>> using a predicate
type private PartitionedEnumerable<'T> private (splitNext : unit -> bool, source : IEnumerable<'T>) = 
    let e = source.GetEnumerator()
    let mutable sourceMoveNext = true

    let innerEnumerator =
        { new IEnumerator<'T> with
            member __.MoveNext() : bool = 
                if splitNext() then false
                else
                    sourceMoveNext <- e.MoveNext()
                    sourceMoveNext

            member __.Current : obj = e.Current  :> _
            member __.Current : 'T = e.Current
            member __.Dispose() : unit = () 
            member __.Reset() : unit = invalidOp "Reset" }

    let innerSeq = 
        { new IEnumerable<'T> with
                member __.GetEnumerator() : IEnumerator = innerEnumerator :> _
                member __.GetEnumerator() : IEnumerator<'T> = innerEnumerator }

    let outerEnumerator =
        { new IEnumerator<IEnumerable<'T>> with
                member __.Current: IEnumerable<'T> = innerSeq
                member __.Current: obj = innerSeq :> _
                member __.Dispose(): unit = ()
                member __.MoveNext() = sourceMoveNext
                member __.Reset(): unit = invalidOp "Reset"
        }

    interface IEnumerable<IEnumerable<'T>> with
        member this.GetEnumerator() : IEnumerator = outerEnumerator :> _
        member this.GetEnumerator() : IEnumerator<IEnumerable<'T>> = outerEnumerator :> _ 

    static member ofSeq (splitNext : unit -> bool) (source : seq<'T>) : seq<seq<'T>> =
        new PartitionedEnumerable<'T>(splitNext, source) :> _

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
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer option

    internal new (path, count, serializer) = 
        let uuid = Guid.NewGuid().ToString()
        { uuid = uuid ; path = path ; count = count ; serializer = serializer }

    member private c.GetSequenceFromStore(config : CloudFileStoreConfiguration) = local {
        let serializer = match c.serializer with Some c -> c | None -> config.Serializer
        let! stream = ofAsync <| config.FileStore.BeginRead(c.path)
        return serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
    }

    /// Returns an enumerable that lazily fetches elements of the cloud sequence from store.
    member c.ToEnumerable () = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        match config.Cache |> Option.bind(fun ch -> ch.TryFind c.uuid) with
        | Some v -> return v :?> seq<'T>
        | None -> return! c.GetSequenceFromStore(config)
    }

    /// Fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArray () : Local<'T []> = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        match config.Cache |> Option.bind(fun ch -> ch.TryFind c.uuid) with
        | Some v -> return v :?> 'T []
        | None ->
            let! seq = c.GetSequenceFromStore(config)
            return Seq.toArray seq
    }

    /// Cache contents to local execution context. Returns true iff succesful.
    member c.PopulateCache () = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        match config.Cache with
        | None -> return false
        | Some ch when ch.ContainsKey c.uuid -> return true
        | Some ch ->
            let! seq = c.GetSequenceFromStore(config)
            let array = Seq.toArray seq
            return ch.Add(c.uuid, array)
    }

    /// Indicates if array is cached in local execution context
    member c.IsCachedLocally = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        return config.Cache |> Option.exists(fun ch -> ch.ContainsKey c.uuid)
    }

    /// Path to Cloud sequence in store
    member c.Path = c.path

    /// Cloud sequence element count
    member c.Count = local {
        match c.count with
        | Some l -> return l
        | None ->
            // this is a potentially costly operation
            let! seq = c.ToEnumerable()
            let l = Seq.length seq
            c.count <- Some l
            return l
    }

    /// Underlying sequence size in bytes
    member c.Size = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.GetFileSize c.path
    }

    interface ICloudStorageEntity with
        member c.Type = sprintf "CloudSequence:%O" typeof<'T>
        member c.Id = c.path

    interface ICloudDisposable with
        member c.Dispose () = local {
            let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
            return! ofAsync <| config.FileStore.DeleteFile c.path
        }

    override c.ToString() = sprintf "CloudSequence[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()

#nowarn "444"

type CloudSequence =

    /// <summary>
    ///     Creates a new Cloud sequence with given values in the underlying store.
    ///     Cloud sequences are cached locally for performance.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member New(values : seq<'T>, ?directory, ?serializer) : Local<CloudSequence<'T>> = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        let directory = defaultArg directory config.DefaultDirectory
        let _serializer = defaultArg serializer config.Serializer
        return! ofAsync <| async {
            let path = config.FileStore.GetRandomFilePath directory
            let writer (stream : Stream) = async {
                return _serializer.SeqSerialize<'T>(stream, values, leaveOpen = false)
            }
            let! length = config.FileStore.Write(path, writer)
            return new CloudSequence<'T>(path, Some length, serializer)
        }
    }

    /// <summary>
    ///     Creates a collection of Cloud sequences partitioned by file size.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per Cloud sequence partition.</param>
    /// <param name="directory"></param>
    /// <param name="serializer"></param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    static member NewPartitioned(values : seq<'T>, maxPartitionSize, ?directory, ?serializer) : Local<CloudSequence<'T> []> = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        let directory = defaultArg directory config.DefaultDirectory
        let _serializer = defaultArg serializer config.Serializer
        return! ofAsync <| async {
            if maxPartitionSize <= 0L then return invalidArg "maxPartitionSize" "Must be greater that 0."

            let seqs = new ResizeArray<CloudSequence<'T>>()
            let currentStream = ref Unchecked.defaultof<Stream>
            let splitNext () = currentStream.Value.Position >= maxPartitionSize
            let partitionedValues = PartitionedEnumerable.ofSeq splitNext values
            for partition in partitionedValues do
                let path = config.FileStore.GetRandomFilePath directory
                let writer (stream : Stream) = async {
                    currentStream := stream
                    return _serializer.SeqSerialize<'T>(stream, partition, leaveOpen = false)
                }
                let! length = config.FileStore.Write(path, writer)
                let seq = new CloudSequence<'T>(path, Some length, serializer)
                seqs.Add seq

            return seqs.ToArray()
        }
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member Parse<'T>(path : string, ?serializer : ISerializer, ?force : bool) : Local<CloudSequence<'T>> = local {
        let! config = Workflow.GetResource<CloudFileStoreConfiguration> ()
        let _serializer = match serializer with Some s -> s | None -> config.Serializer
        let cseq = new CloudSequence<'T>(path, None, serializer)
        if defaultArg force false then
            let! _ = cseq.Count in ()
        else
            let! exists = ofAsync <| config.FileStore.FileExists path
            if not exists then return raise <| new FileNotFoundException(path)
            
        return cseq
    }

    /// <summary>
    ///     Parses an already existing sequence of given type in provided file store.
    /// </summary>
    /// <param name="file">Target cloud file.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member Parse<'T>(file : CloudFile, ?serializer : ISerializer, ?force : bool) : Local<CloudSequence<'T>> =
        CloudSequence.Parse<'T>(file, ?serializer = serializer, ?force = force)

    /// <summary>
    ///     Creates a CloudSequence from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromFile<'T>(path : string, deserializer : Stream -> seq<'T>, ?force : bool) : Local<CloudSequence<'T>> = local {
        let serializer =
            {
                new ISerializer with
                    member __.Id = "Deserializer Lambda"
                    member __.Serialize(_,_,_) = raise <| new NotSupportedException()
                    member __.Deserialize(_,_) = raise <| new NotSupportedException()
                    member __.SeqSerialize(_,_,_) = raise <| new NotSupportedException()
                    member __.SeqDeserialize<'a>(source,_) = deserializer source :> obj :?> seq<'a>
            }

        return! CloudSequence.Parse(path, serializer, ?force = force)
    }

    /// <summary>
    ///     Creates a CloudSequence from text file.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromTextFile<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, ?encoding : Encoding, ?force : bool) : Local<CloudSequence<'T>> = local {
        let deserializer (stream : Stream) =
            let sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 
        
        return! CloudSequence.FromFile(path, deserializer, ?force = force)
    }

    /// <summary>
    ///     Creates a CloudSequence from file path with user-provided deserialization function.
    /// </summary>
    /// <param name="file">Target cloud file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Force evaluation. Defaults to false.</param>
    static member FromFile<'T>(file : CloudFile, deserializer : Stream -> seq<'T>, ?force) : Local<CloudSequence<'T>> =
        CloudSequence.FromFile<'T>(file.Path, deserializer = deserializer, ?force = force)


[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CloudSequence =
    
    /// <summary>
    ///     Returns an enumerable that lazily fetches elements of the cloud sequence from store.
    /// </summary>
    /// <param name="cseq">Input cloud sequence</param>
    let toSeq (cseq : CloudSequence<'T>) = cseq.ToEnumerable()

    /// <summary>
    ///     Fetches all elements of the cloud sequence and returns them as a local array.
    /// </summary>
    /// <param name="cseq">Input cloud sequence</param>
    let toArray (cseq : CloudSequence<'T>) = cseq.ToArray()

    /// <summary>
    ///     Cache contents to local execution context. Returns true iff succesful.
    /// </summary>
    /// <param name="cseq">Input cloud sequence.</param>
    let cache (cseq : CloudSequence<'T>) = cseq.PopulateCache()