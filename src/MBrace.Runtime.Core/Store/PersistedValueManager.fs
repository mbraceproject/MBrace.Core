namespace MBrace.Runtime.Store

open System
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Runtime.InMemoryRuntime

#nowarn "444"

/// Represents an object entity that is either persisted in cloud store
/// or comes as an encapsulated pickle if sufficiently small
type IPickleOrFile =
    /// Gets the value size in bytes
    abstract Size : int64
    abstract Type : Type
    abstract IsEncapsulated : bool
    abstract IsPersisted : bool
    abstract GetValueBoxed : unit -> obj
    abstract GetValueBoxedAsync : unit -> Async<obj>

/// Represents an object entity that is either persisted in cloud store
/// or comes as an encapsulated pickle if sufficiently small
[<NoEquality; NoComparison>]
type PickleOrFile<'T> =
    | EncapsulatedPickle of byte [] * ISerializer
    | Persisted of PersistedValue<'T>
with
    /// Gets the value size in bytes
    member fp.Size =
        match fp with
        | Persisted pv -> pv.Size
        | EncapsulatedPickle (p,_) -> int64 p.Length

    /// Dereferences the value
    member fp.Value =
        match fp with
        | EncapsulatedPickle (p,s) -> s.UnPickle<'T> p
        | Persisted pf -> pf.Value

    /// Asynchronously dereferences the value
    member fp.GetValueAsync() = async {
        match fp with
        | EncapsulatedPickle (p,s) -> return s.UnPickle<'T> p
        | Persisted pf -> return! pf.GetValueAsync()
    }

    interface IPickleOrFile with
        member __.Size = __.Size
        member __.Type = typeof<'T>
        member __.IsEncapsulated = match __ with EncapsulatedPickle _ -> true | _ -> false
        member __.IsPersisted = match __ with Persisted _ -> true | _ -> false
        member __.GetValueBoxed () = __.Value :> obj
        member __.GetValueBoxedAsync () = async {
            let! r = __.GetValueAsync()
            return r :> obj
        }


/// Provides utility methods for persisting .NET objects to files in the cloud store.
/// Can be safely serialized.
[<Sealed; DataContract>]
type PersistedValueManager private (resources : ResourceRegistry, persistThreshold : int64) =

    [<DataMember(Name = "Resources")>]
    let resources = resources

    [<DataMember(Name = "PersistThreshold")>]
    let _persistThreshold = persistThreshold

    let toAsync (workflow : Local<'T>) = Cloud.ToAsyncWithCurrentCancellationToken(workflow, resources)

    let getPath (fileName : string) = local {
        let! dir = CloudPath.DefaultDirectory
        return! CloudPath.Combine(dir, fileName)
    }

    /// <summary>
    ///     Creates a new persisted value manager with supplied configuration.
    /// </summary>
    /// <param name="fileStore">FileStore instance used for persisting.</param>
    /// <param name="container">Container directory for placing persisted values.</param>
    /// <param name="serializer">Default serializer used for persisting values.</param>
    /// <param name="persistThreshold">Threshold in bytes after which values are persisted as files.</param>
    static member Create(fileStore : ICloudFileStore, container : string, serializer : ISerializer, persistThreshold : int64) =
        let resources = resource {
            yield CloudFileStoreConfiguration.Create(fileStore, defaultDirectory = container)
            yield serializer
        }

        new PersistedValueManager(resources, persistThreshold)

    /// <summary>
    ///     Asynchronously persists provided value to file of given name.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="fileName">Filename to persist to. Must be relative path.</param>
    /// <param name="serializer">Serializer used for persisting. Defaults to the resource serializer.</param>
    member __.PersistValueAsync(value : 'T, fileName : string, ?serializer : ISerializer) : Async<PersistedValue<'T>> =
        local {
            let! path = getPath fileName
            return! PersistedValue.New(value, path = path, ?serializer = serializer) 
        } |> toAsync

    /// <summary>
    ///     Creates an entity that is either persisted as file or encapsulated in entity, depending on provided size threshold.
    /// </summary>
    /// <param name="value">Value to be persisted.</param>
    /// <param name="fileName">Filename to persist to. Must be relative path.</param>
    /// <param name="persistThreshold">File persist threshold in bytes. Objects whose size is greater than the treshold are persisted, otherwise encapsulated.</param>
    /// <param name="serializer">Serializer used for persisting. Defaults to the resource serializer.</param>
    member __.CreateFileOrPickleAsync(value : 'T, fileName : string, ?persistThreshold : int64, ?serializer : ISerializer) : Async<PickleOrFile<'T>> = async {
        let persistThreshold = defaultArg persistThreshold _persistThreshold
        if VagabondRegistry.Instance.Serializer.ComputeSize value > persistThreshold then
            let! pv = __.PersistValueAsync(value, fileName, ?serializer = serializer)
            return Persisted pv
        else
            let serializer = match serializer with Some s -> s | None -> resources.Resolve()
            let pickle = serializer.Pickle value
            return EncapsulatedPickle (pickle, serializer)
    }

    /// <summary>
    ///     Asynchronously persists provided sequence to file of given name.
    /// </summary>
    /// <param name="value">Values to be persisted.</param>
    /// <param name="fileName">Filename to persist to. Must be relative path.</param>
    /// <param name="serializer">Serializer used for persisting. Defaults to the resource serializer.</param>
    member __.PeristSequenceAsync(values : seq<'T>, fileName : string, ?serializer : ISerializer) : Async<PersistedSequence<'T>> =
        local {
            let! dir = CloudPath.DefaultDirectory
            let! path = CloudPath.Combine(dir, fileName)
            return! PersistedSequence.New(values, path = path, ?serializer = serializer) 
        } |> toAsync

    /// <summary>
    ///     Asynchronously attempt to get a persisted value by given type and fileName.
    /// </summary>
    /// <param name="fileName">File name to be looked up.</param>
    member __.TryGetPersistedValueByFileName<'T>(fileName : string) : Async<PersistedValue<'T> option> =
        local {
            let! path = getPath fileName
            try 
                let! pv = PersistedValue.OfCloudFile<'T>(path)
                return Some pv
            with :? System.IO.FileNotFoundException -> 
                return None
        } |> toAsync

    /// <summary>
    ///     Asynchronously attempt to get a persisted sequence by given type and fileName.
    /// </summary>
    /// <param name="fileName">File name to be looked up.</param>
    member __.TryGetPersistedSequenceByFileName<'T>(fileName : string) : Async<PersistedSequence<'T> option> =
        local {
            let! path = getPath fileName
            try 
                let! pc = PersistedSequence.OfCloudFile<'T>(path)
                return Some pc
            with :? System.IO.FileNotFoundException -> 
                return None
        } |> toAsync