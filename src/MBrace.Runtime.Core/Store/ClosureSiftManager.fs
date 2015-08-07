namespace MBrace.Runtime.Store

open System
open System.Collections
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reflection

open Operators.Checked

open Nessos.FsPickler
open Nessos.FsPickler.Hashing
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Utils.Reflection
open MBrace.Runtime.InMemoryRuntime
open MBrace.Runtime.Vagabond

[<AutoSerializable(true); NoEquality; NoComparison>]
type private SiftMethod =
    | CloudValue of ICloudValue
    | VagabondValue of FieldInfo

[<AutoSerializable(true); NoEquality; NoComparison>]
type private SiftedObjectInfo =
    {
        Name : string
        Hash : HashResult
        SiftMethod : SiftMethod
        Indices : int64 []
    }

/// Contains a closure that has been sifted of large values.
[<AutoSerializable(true); NoEquality; NoComparison>]
type SiftedClosure<'T> =
    private
    | Intact of 'T
    | Sifted of Graph:Sifted<'T> * SiftedObjectInfo []

/// Serializable ClosureSiftManager configuration object
[<AutoSerializable(true); NoEquality; NoComparison>]
type ClosureSiftConfiguration =
    {
        /// CloudValueProvider instance used for sifiting large closures
        CloudValueProvider : ICloudValueProvider
        /// Sift threshold for large values in closures (in bytes)
        SiftThreshold : int64
    }
with
    /// <summary>
    ///     Creates a Closure sift configuration object with provided parameters.
    /// </summary>
    /// <param name="cloudValueProvider">Cloud value provider implementation used for storing sifted values.</param>
    /// <param name="siftThreshold">Sift threshold for values inside closures (in bytes). Defaults to 5 MiB.</param>
    static member Create(cloudValueProvider : ICloudValueProvider, ?siftThreshold : int64) =
        let siftThreshold = 
            match siftThreshold with
            | Some st when st <= 0L -> invalidArg "siftThreshold" "Must be positive value."
            | Some st -> st
            | None -> 5L * 1024L * 1024L

        {
            CloudValueProvider = cloudValueProvider
            SiftThreshold = siftThreshold
        }

[<Sealed; AutoSerializable(false)>]
type private LargeObjectSifter(vagabond : VagabondManager, siftThreshold : int64, isSifted : HashResult -> bool, allowNewSifts : bool) =
    let serializer = vagabond.Serializer
    let refEq = new ReferenceEqualityComparer<obj> ()
    let vagabondStaticBindings =
        let d = new Dictionary<obj, FieldInfo * HashResult> (refEq)
        for f,h in vagabond.StaticBindings do
            match f.GetValue(null) with
            | null -> ()
            | o -> d.[o] <- (f,h)
        d

    let siftResult = new Dictionary<obj, HashResult * FieldInfo option> (refEq)

    member __.SiftData = siftResult

    interface IObjectSifter with
        member x.Sift(_ : Pickler<'T>, _ : int64, node: 'T): bool =
            let node = box node
            match vagabondStaticBindings.TryFind node with
            | Some (fieldInfo, hash) when hash.Length > siftThreshold ->
                // large value identified as vagabond data dependency; 
                // declare sifted and update sift results
                siftResult.Add(node, (hash, Some fieldInfo))
                true

            | _ ->

            match box node with
            // Identify values identified as large that require sifting
            // The following objects are to be sifted (append as appropriate)
            //   * large System.Collections.ICollection instances
            //   * large F# collections (which do not implement ICollection)
            | CollectionWithCount(enum, count) when count > 1 ->
                let hash = serializer.ComputeHash node
                let siftNode () = siftResult.Add(node, (hash, None)) ; true

                if isSifted hash then
                    // value already declared sifted in session; append as appropriate.
                    siftNode ()

                elif hash.Length > siftThreshold && allowNewSifts then
                    // not all collections of large serialization size may be 'large' in their own right.
                    // In many cases a large object may simply be encapsulated further down the object graph.
                    // Perform a simple heuristic to identify what is the case here.
                    // We traverse a small segment of the collection, computing an expected serialization
                    // size for each element. We use this to attain a 'replication metric' for the collection graph.

                    let averageElementSize = hash.Length / int64 count

                    // large collections that have elements of small average size will always be sifted
                    if averageElementSize < 256L then siftNode () else

                    // perform replication test by sampling individual elements of the collection
                    let averageSampleSize =
                        let e = enum.GetEnumerator()
                        let sizeCounter = serializer.CreateObjectSizeCounter()
                        let sizes = new ResizeArray<int64> ()
                        let mutable currSize = 0L
                        let sizeThreshold = 3L * hash.Length
                        while e.MoveNext() && currSize < sizeThreshold && sizeCounter.ObjectCount < 1000L do
                            sizeCounter.Append e.Current
                            let count = sizeCounter.Count
                            sizes.Add(count - currSize)
                            currSize <- count
                            sizeCounter.ResetSerializationCache() // clear FsPickler ObjectCache so that recurring objects are always taken into account

                        Seq.sum sizes / int64 sizes.Count

                    // a collection demonstrates replication of data 
                    // (i.e. has a large object referenced further down the object graph referenced by multiple elements)
                    // if the sampled element size greatly exceeds the projected average then this is a clear indication
                    // of replication and thus the current array will not be sifted.
                    if averageSampleSize > 2L * averageElementSize then false
                    else
                        siftNode ()
                else
                    false

            | _ -> false
        

/// Management object used for optimizing large closures using CloudValue and Vagabond.
/// Type is *not* serializable, transfer using the StoreAssemblyManagerConfiguration object instead.
[<Sealed; AutoSerializable(false)>]
type ClosureSiftManager private (cloudValueProvider : ICloudValueProvider, siftThreshold : int64, logger : ISystemLogger) =

    let vagabond = VagabondRegistry.Instance
    let localSifts = new ConcurrentDictionary<HashResult, unit>()
    let isSifted hash = localSifts.ContainsKey hash
    let append hash = ignore <| localSifts.TryAdd(hash, ())

    /// <summary>
    ///     Creates a local ClosureSiftManager instance with provided configuration object.
    /// </summary>
    /// <param name="configuration">Configuration object used for sifting closures.</param>
    /// <param name="localLogger">Local system logger used by manager. Defaults to no logging.</param>
    static member Create(configuration : ClosureSiftConfiguration, ?localLogger : ISystemLogger) =
        let localLogger = match localLogger with Some l -> l | None -> new NullLogger() :> _
        new ClosureSiftManager(configuration.CloudValueProvider, configuration.SiftThreshold, localLogger)
    
    /// <summary>
    ///     Creates a sifted closure for given value if necessary.
    /// </summary>
    /// <param name="value">Value to be sifted.</param>
    /// <param name="allowNewSifts">
    ///     If enabled, large values will be sifted and created as cloud values even if they
    ///     do not already exist as such.
    /// </param>
    member __.SiftClosure(value : 'T, allowNewSifts : bool) : Async<SiftedClosure<'T>> = async {
        if vagabond.Serializer.ComputeSize value > siftThreshold then
            let sifter = new LargeObjectSifter(vagabond, siftThreshold, isSifted, allowNewSifts)
            let siftedClosure, siftedObjects = FsPickler.Sift(value, sifter)
            let groupedSifts = 
                siftedObjects 
                |> Seq.map (fun (id,o) -> let hash, vgbField = sifter.SiftData.[o] in id,o,hash,vgbField)
                |> Seq.groupBy (fun (_,_,hash,_) -> hash)
                |> Seq.map (fun (hash,entries) -> 
                                let indices = entries |> Seq.map (fun (id,_,_,_) -> id) |> Seq.toArray
                                let _,o,_,isVagabond = Seq.head entries
                                o,indices,hash,isVagabond)
                |> Seq.toArray

            let mkSiftedNode (value : obj, indices : int64[], hash : HashResult, vagabondField : FieldInfo option) = async {
                let typeName = Type.prettyPrint <| value.GetType()
                let! siftMethod = async {
                    match vagabondField with
                    | Some f ->
                        logger.Logf LogLevel.Debug "Sifting value of type '%s' and size %s using Vagabond." typeName <| getHumanReadableByteSize hash.Length
                        return VagabondValue f
                    | None ->
                        let! cv = cloudValueProvider.CreateCloudValue(value, storageLevel = StorageLevel.MemoryAndDisk)
                        logger.Logf LogLevel.Debug "Sifting value of type '%s' and size %s using CloudValue." typeName <| getHumanReadableByteSize hash.Length
                        do append hash
                        return CloudValue cv
                }

                return { Hash = hash ; Indices = indices ; SiftMethod = siftMethod ; Name = typeName }
            }

            let! siftedInfo = groupedSifts |> Seq.map mkSiftedNode |> Async.Parallel
            if siftedInfo.Length = 0 then return Intact value
            else
                return Sifted(siftedClosure, siftedInfo)
        else
            return Intact value
    }

    /// <summary>
    ///     Recovers the original closure by substituting sifted values in the local context.
    /// </summary>
    /// <param name="siftedClosure">Closure to be unsifted.</param>
    member __.UnSiftClosure(siftedClosure : SiftedClosure<'T>) : Async<'T> = async {
        match siftedClosure with
        | Intact t -> return t
        | Sifted(siftedClosure, siftedObjectInfo) ->
            let getSiftedValue (info : SiftedObjectInfo) = async {
                match info.SiftMethod with
                | VagabondValue f ->
                    logger.Logf LogLevel.Debug "Loading sifted value of type '%s' and size %s using Vagabond." info.Name <| getHumanReadableByteSize info.Hash.Length
                    let value = f.GetValue(null)
                    return info, value

                | CloudValue cv ->
                    do append info.Hash
                    logger.Logf LogLevel.Debug "Loading sifted value of type '%s' and size %s using CloudValue." info.Name <| getHumanReadableByteSize info.Hash.Length
                    let! value = cv.GetBoxedValueAsync()
                    return info, value
            }

            let! siftedValues = siftedObjectInfo |> Seq.map getSiftedValue |> Async.Parallel
            let extractedSifts =
                siftedValues
                |> Seq.collect (fun (info, v) -> info.Indices |> Seq.map (fun i -> (i,v)))
                |> Seq.toArray

            return FsPickler.UnSift<'T>(siftedClosure, extractedSifts)
    }