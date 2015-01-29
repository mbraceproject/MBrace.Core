namespace MBrace.Streams

open System
open System.Collections
open System.Collections.Generic

open MBrace.Store
open MBrace.Continuation
open MBrace
open Nessos.Streams
open System.Runtime.Serialization

// TODO : Persist CloudArray Descriptor in store.
// TODO : Implement CloudArray.FromPath
// TODO : Implement proper dispose.
// TODO : Add .Path and maybe expose Partition<'T> type.
// TODO : CloudArray.Merge : CloudArray<'T> [] -> CloudArray<'T> ?

[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type internal Partition<'T> =

    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "StartIndex")>]
    val mutable private startIndex : int64
    [<DataMember(Name = "EndIndex")>]
    val mutable private endIndex : int64
    [<DataMember(Name = "FileStore")>]
    val mutable private fileStore : ICloudFileStore
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer

    internal new (path, startIndex, endIndex, fileStore, serializer) =
        { path = path ; startIndex = startIndex ; endIndex = endIndex ; serializer = serializer ; fileStore = fileStore }
    
    member private p.GetSequence() = 
        async {
            let! stream = p.fileStore.BeginRead p.path
            return p.serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        } |> Async.RunSync

    override p.ToString () = sprintf "Partition[%d,%d] %s" p.startIndex p.endIndex p.path
    member private p.StructuredFormatDisplay = p.ToString()

    member internal p.Serializer = p.serializer
    member internal p.FileStore  = p.fileStore

    /// Path to Partition in store.
    member p.Path = p.path
    /// Partition length.
    member p.Length = int(p.endIndex - p.startIndex + 1L)
    /// Index of the first element.
    member p.StartIndex = p.startIndex
    /// Index of the last element.
    member p.EndIndex = p.endIndex
    /// Read the entire partition.
    member p.ToArray() =
        let array = Array.zeroCreate<'T> p.Length
        let mutable i = 0
        for item in p.GetSequence() do
            array.[i] <- item
            i <- i + 1
        array

    member internal p.OffsetBy(offset : int64) =
        new Partition<'T>(p.Path, p.StartIndex + offset, p.EndIndex + offset, p.FileStore, p.Serializer)

    interface ICloudDisposable with
        member p.Dispose() = 
            p.fileStore.DeleteFile p.path
            |> Cloud.OfAsync

    interface IEnumerable<'T> with
        member p.GetEnumerator() = (p.GetSequence() :> IEnumerable).GetEnumerator()
        member p.GetEnumerator() = p.GetSequence().GetEnumerator()


/// CloudArray description.
type internal Descriptor<'T> = 
    { Length : int64
      Id : string
      Partitions : Partition<'T> [] }

/// Represents a finite and immutable and ordered sequence of
/// elements that is stored in the underlying CloudStore
/// in partitions.
[<AutoSerializable(true) ; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudArray<'T> internal (root : Descriptor<'T>) =

    [<DataMember(Name = "Descriptor")>]
    let root = root

    member internal __.Descriptor = root
    member internal __.Id = root.Id
    member internal __.Partitions = root.Partitions
    member private __.StructuredFormatDisplay = __.ToString()
    override __.ToString() = sprintf "CloudArray : %s" __.Id

    /// Number of partitions.
    member this.PartitionCount = root.Partitions.Length

    /// Length of the cloud array.
    member this.Length = root.Length

    /// Builds a new cloud array that contains partitions from the first
    /// cloud array followed by partitions of the second cloud array.
    member this.Append(array : CloudArray<'T>) =
        let offsetPartitions = array.Partitions |> Array.map (fun p -> p.OffsetBy(this.Length) )
        let root = { Id = Guid.NewGuid().ToString("N")
                     Length = array.Length + this.Length
                     Partitions = Array.append root.Partitions offsetPartitions }
        new CloudArray<'T>(root)

    /// Fetch nth partition as an array.
    member this.GetPartition(index : int) : 'T [] = root.Partitions.[index].ToArray()

    member this.Item
        with get (index : int64) : 'T =
            let i, partition = 
                root.Partitions
                |> Seq.mapi (fun i e -> i,e)
                |> Seq.find (fun (_,p) -> p.StartIndex <= index && index <= p.EndIndex) 
            let relativeIndex = int (index - partition.StartIndex)
            Seq.nth relativeIndex partition

    interface ICloudDisposable with
        member __.Dispose() = 
            cloud { 
                do! root.Partitions 
                    |> Seq.map (fun p -> (p :> ICloudDisposable).Dispose()) // Wrong
                    |> Cloud.Parallel
                    |> Cloud.Ignore 
            } 

    interface IEnumerable<'T> with
        member __.GetEnumerator() = 
            (__ :> IEnumerable<'T>).GetEnumerator() :> IEnumerator
        member __.GetEnumerator() = 
           (root.Partitions |> Seq.collect id).GetEnumerator()

    static member CreateAsync (values : seq<'T>, directory : string, fileStore : ICloudFileStore, serializer : ISerializer, ?partitionSize) = async {
        let maxPartitionSize = defaultArg partitionSize (1024L * 1024L * 1024L) 
        if maxPartitionSize <= 0L then return invalidArg "partitionSize" "Must be greater that 0."

        let currentStream = ref Unchecked.defaultof<System.IO.Stream>
        let pred () = currentStream.Value.Position < maxPartitionSize
        
        let partitioned = PartitionedEnumerable.ofSeq pred values

        let partitions = new ResizeArray<Partition<'T>>()
        let index = ref 0L
        for partition in partitioned do
            let fileName = fileStore.GetRandomFilePath directory
            let! length = fileStore.Write(fileName, fun stream -> async {
                    currentStream := stream
                    return serializer.SeqSerialize(stream, partition, leaveOpen = false) 
                })
            
            let length = int64 length

            let partition = new Partition<'T>(fileName, !index, !index + length - 1L, fileStore, serializer)
            partitions.Add(partition)
            index := !index + length 

        let root = { Id = Guid.NewGuid().ToString("N") ; Length = !index; Partitions = partitions.ToArray() }

        return new CloudArray<'T>(root)
    }


#nowarn "444"

/// Provides methods on CloudArrays.
type CloudArray =
    
    /// <summary>
    /// Create a new cloud array.
    /// </summary>
    /// <param name="values">Collection to populate the cloud array with.</param>
    /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="partitionSize">Approximate partition size in bytes.</param>
    static member New(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) = cloud {
        let! fs = Cloud.GetResource<CloudFileStoreConfiguration>()
        let! serializer = cloud {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let directory = match directory with None -> fs.DefaultDirectory | Some d -> d

        return! Cloud.OfAsync <| CloudArray<'T>.CreateAsync(values, directory, fs.FileStore, serializer, ?partitionSize = partitionSize)
    }
