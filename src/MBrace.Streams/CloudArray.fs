namespace MBrace.Streams

open System
open System.Collections
open System.Collections.Generic

open MBrace.Store
open MBrace.Continuation
open MBrace
open Nessos.Streams
open System.Runtime.Serialization

#nowarn "0443"
#nowarn "0444"

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
    [<DataMember(Name = "Serializer")>]
    val mutable private serializer : ISerializer option

    internal new (path, startIndex, endIndex, serializer) =
        { path = path ; startIndex = startIndex ; endIndex = endIndex ; serializer = serializer }
    
    member private p.GetValueFromStore(config : CloudFileStoreConfiguration) = 
        async {
            let serializer = match p.serializer with Some s -> s | None -> config.Serializer
            let! stream = config.FileStore.BeginRead p.path
            return serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        } 

    override p.ToString () = sprintf "Partition[%d,%d] %s" p.startIndex p.endIndex p.path
    member private p.StructuredFormatDisplay = p.ToString()

    member internal p.Serializer = p.serializer

    /// Path to Partition in store.
    member p.Path = p.path
    /// Partition length.
    member p.Length = int(p.endIndex - p.startIndex + 1L)
    /// Index of the first element.
    member p.StartIndex = p.startIndex
    /// Index of the last element.
    member p.EndIndex = p.endIndex
    /// Read the entire partition.
    member p.ToArray() = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        let array = Array.zeroCreate<'T> p.Length
        let i = ref 0
        let! s = Cloud.OfAsync <| p.GetValueFromStore(config)
        for item in s do
            array.[!i] <- item
            incr i
        return array
    }
    /// Lazily read the partition.
    member p.ToEnumerable() = cloud {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        return! Cloud.OfAsync <| p.GetValueFromStore(config)
    }

    member internal p.OffsetBy(offset : int64) =
        new Partition<'T>(p.Path, p.StartIndex + offset, p.EndIndex + offset, p.Serializer)

    interface ICloudDisposable with
        member p.Dispose() =  cloud {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
            return! config.FileStore.DeleteFile p.path
                    |> Cloud.OfAsync
        }

//    interface IEnumerable<'T> with
//        member p.GetEnumerator() = (p.GetSequence() :> IEnumerable).GetEnumerator()
//        member p.GetEnumerator() = p.GetSequence().GetEnumerator()


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
    member this.GetPartition(index : int) : Cloud<'T []> = root.Partitions.[index].ToArray()

    member this.Item
        with get (index : int64) : Cloud<'T> =
            cloud {
                let i, partition = 
                    root.Partitions
                    |> Seq.mapi (fun i e -> i,e)
                    |> Seq.find (fun (_,p) -> p.StartIndex <= index && index <= p.EndIndex) 
                let relativeIndex = int (index - partition.StartIndex)
                let! s = partition.ToEnumerable()
                return Seq.nth relativeIndex s
            }

    interface ICloudDisposable with
        member __.Dispose() = 
            cloud { 
                do! root.Partitions 
                    |> Seq.map (fun p -> (p :> ICloudDisposable).Dispose()) // Wrong
                    |> Cloud.Parallel
                    |> Cloud.Ignore 
            } 
    member p.ToEnumerable() =
        cloud {
            let! resources = Cloud.GetResourceRegistry()
            return root.Partitions |> Seq.collect (fun p -> Cloud.RunSynchronously(p.ToEnumerable(), resources, new MBrace.Runtime.InMemory.InMemoryCancellationToken()))
        }

    static member CreateAsync (values : seq<'T>, directory : string, config : CloudFileStoreConfiguration, ?serializer : ISerializer, ?partitionSize) = async {
        let maxPartitionSize = defaultArg partitionSize (1024L * 1024L * 1024L) 
        if maxPartitionSize <= 0L then return invalidArg "partitionSize" "Must be greater that 0."

        let _serializer = defaultArg serializer config.Serializer
        let currentStream = ref Unchecked.defaultof<System.IO.Stream>
        let pred () = currentStream.Value.Position < maxPartitionSize
        
        let partitioned = PartitionedEnumerable.ofSeq pred values

        let partitions = new ResizeArray<Partition<'T>>()
        let index = ref 0L
        for partition in partitioned do
            let fileName = config.FileStore.GetRandomFilePath directory
            let! length = config.FileStore.Write(fileName, fun stream -> async {
                    currentStream := stream
                    return _serializer.SeqSerialize(stream, partition, leaveOpen = false) 
                })
            
            let length = int64 length

            let partition = new Partition<'T>(fileName, !index, !index + length - 1L, serializer)
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
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        
        let directory = match directory with None -> config.DefaultDirectory | Some d -> d

        return! Cloud.OfAsync <| CloudArray<'T>.CreateAsync(values, directory, config, ?serializer = serializer, ?partitionSize = partitionSize)
    }


[<AutoOpen>]
module StoreClientExtensions =
    open System.Runtime.CompilerServices
    
    /// Common operations on CloudArrays.
    type CloudArrayClient internal (resources : ResourceRegistry) =
        let toAsync wf = Cloud.ToAsync(wf, resources)
        let toSync wf = Cloud.RunSynchronously(wf, resources, new MBrace.Runtime.InMemory.InMemoryCancellationToken())

        /// <summary>
        /// Create a new cloud array.
        /// </summary>
        /// <param name="values">Collection to populate the cloud array with.</param>
        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
        /// <param name="partitionSize">Approximate partition size in bytes.</param>
        member __.NewAsync(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) =
            CloudArray.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
            |> toAsync
    
        /// <summary>
        /// Create a new cloud array.
        /// </summary>
        /// <param name="values">Collection to populate the cloud array with.</param>
        /// <param name="directory">FileStore directory used for cloud seq. Defaults to execution context.</param>
        /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
        /// <param name="partitionSize">Approximate partition size in bytes.</param>
        member __.New(values : seq<'T> , ?directory : string, ?partitionSize, ?serializer : ISerializer) =
            CloudArray.New(values, ?directory = directory, ?partitionSize = partitionSize, ?serializer = serializer)
            |> toSync
    
    [<Extension>]
    type MBrace.Client.StoreClient with
        [<Extension>]
        /// CloudArray client.
        member this.CloudArray = new CloudArrayClient(this.Resources)