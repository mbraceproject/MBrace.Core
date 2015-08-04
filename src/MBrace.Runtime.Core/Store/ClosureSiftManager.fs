namespace MBrace.Runtime.Store

open System
open System.Collections
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reflection

open Nessos.FsPickler
open Nessos.FsPickler.Hashing
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.InMemoryRuntime
open MBrace.Runtime.Vagabond

[<AutoSerializable(true); NoEquality; NoComparison>]
type private SiftedNode =
    | Encapsulated of HashResult * obj
    | VagabondValue of HashResult
    | CloudValue of HashResult * ICloudValue

/// Contains a closure that has been sifted of large values.
[<AutoSerializable(true); NoEquality; NoComparison>]
type SiftedClosure<'T> =
    private
    | Intact of 'T
    | Sifted of HashSift<'T> * SiftedNode []

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
type private LargeObjectSifter(serializer : FsPicklerSerializer, rootObj : obj, siftThreshold : int64) =
    let siftedHashes = new Dictionary<HashResult, ResizeArray<int64>> ()

    interface IObjectSifter with
        member x.Sift(pickler: Pickler<'T>, id: int64, node: 'T): bool =
            if obj.ReferenceEquals(node, rootObj) then false else

            match box node with
            | :? ICollection as collection ->
                let x = collection.Count
                
//            | :? IEnumerable when isList node ->
//                let hash = serializer.ComputeHash node
//                if hash.Length > siftThreshold then
                    

            | _ -> false   
        

/// Local instance used for sifting large object graphs.
[<Sealed; AutoSerializable(false)>]
type ClosureSiftManager private (cloudValueProvider : ICloudValueProvider, siftThreshold : int64) =

    let vagabond = VagabondRegistry.Instance
    let localSifts = new ConcurrentDictionary<HashResult, unit>()
    let isSifted hash = localSifts.ContainsKey hash
    let append hash = ignore <| localSifts.TryAdd(hash, ())
    let logger = new AttacheableLogger()

    /// <summary>
    ///     Creates a local ClosureSiftManager instance with provided configuration object.
    /// </summary>
    /// <param name="configuration">Configuration object used for sifting closures.</param>
    /// <param name="localLogger">Local system logger used by manager. Defaults to no logging.</param>
    static member Create(configuration : ClosureSiftConfiguration) =
        new ClosureSiftManager(configuration.CloudValueProvider, configuration.SiftThreshold)
    
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
            let shouldSiftObject (node:obj) (hash:HashResult) =
                if obj.ReferenceEquals(value, node) then false
                elif isSifted hash then true
                elif allowNewSifts then
                    match node with
                    | :? System.Collections.ICollection -> hash.Length > siftThreshold
                    | _ when isList node -> hash.Length > siftThreshold
                    | _ -> false
                else
                    false

            let siftedClosure, siftedObjects = vagabond.Serializer.HashSift(value, shouldSiftObject)
            let mkSiftedNode (value:obj, hash:HashResult) = async {
                match vagabond.TryGetBindingByHash hash with
                | Some _ -> 
                    let typeName = Type.prettyPrint <| value.GetType()
                    logger.Logf LogLevel.Info "Sifting value of type '%s' and size %s using Vagabond." typeName <| getHumanReadableByteSize hash.Length
                    do append hash
                    return VagabondValue hash
                | None when allowNewSifts ->    
                    let! cv = cloudValueProvider.CreateCloudValue(value, storageLevel = StorageLevel.MemoryAndDisk)
                    let typeName = Type.prettyPrint <| value.GetType()
                    logger.Logf LogLevel.Info "Sifting value of type '%s' and size %s using CloudValue store." typeName <| getHumanReadableByteSize hash.Length
                    do append hash
                    return CloudValue(hash, cv)

                | None ->
                    let id = cloudValueProvider.GetCloudValueId value
                    let! result = cloudValueProvider.TryGetCloudValueById id
                    match result with
                    | Some cv -> 
                        do append hash
                        return CloudValue(hash, cv)
                    | None -> return Encapsulated(hash, value)
            }

            let! siftedNodes = siftedObjects |> Seq.map mkSiftedNode |> Async.Parallel
            if siftedNodes.Length = 0 then return Intact value
            else
                return Sifted(siftedClosure, siftedNodes)

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
        | Sifted(siftedClosure, siftedNodes) ->
            let getSiftedValue (node : SiftedNode) = async {
                match node with
                | Encapsulated(hash,obj) -> return obj, hash
                | VagabondValue hash -> 
                    match vagabond.TryGetBindingByHash hash with
                    | Some f -> 
                        logger.Logf LogLevel.Info "Loading sifted dependency '%s' from Vagabond." hash.Id
                        do append hash
                        return f.GetValue(null), hash

                    | None -> return invalidOp <| sprintf "ClosureSiftManager: could not locate vagabond binding for binding with hash '%s'." hash.Id

                | CloudValue(hash, cv) ->
                    do append hash
                    logger.Logf LogLevel.Info "Loading sifted dependency '%s' from CloudValue store." cv.Id
                    let! obj = cv.GetBoxedValueAsync()
                    return obj, hash
            }

            let! siftedObjects = siftedNodes |> Seq.map getSiftedValue |> Async.Parallel
            return vagabond.Serializer.HashUnsift<'T>(siftedClosure, siftedObjects)
    }

    /// <summary>
    ///     Attaches a logger instance to the sift manager instance.
    /// </summary>
    /// <param name="newLogger">Logger to be attached.</param>
    member __.AttachLogger(newLogger : ISystemLogger) = logger.AttachLogger newLogger