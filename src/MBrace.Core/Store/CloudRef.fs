namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

type private CloudRefStorageSource =
    | Table of id:string
    | File of serializerId:string * path:string

/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references are cached locally for performance.
[<Sealed; AutoSerializable(true)>]
type CloudRef<'T> private (init : 'T, source : CloudRefStorageSource, store : ICloudStore) =

    // conveniently, the uninitialized value for optional fields coincides with 'None'.
    // a more correct approach would initialize using an OnDeserialized callback
    [<NonSerialized>]
    let mutable cachedValue = Some init
    [<NonSerialized>]
    let mutable localStore = Some store
    let storeId = store.UUID
    // delayed store bootstrapping after deserialization
    let getStore() =
        match localStore with
        | Some s -> s
        | None ->
            let s = CloudStoreRegistry.Resolve storeId
            localStore <- Some s
            s

    let getValue () = async {
        match source with
        | Table id -> return! getStore().TableStore.Value.GetValue id
        | File(serializerId, path) ->
            let serializer = SerializerRegistry.Resolve serializerId
            use! stream = getStore().FileStore.BeginRead path
            return serializer.Deserialize<'T>(stream)
    }

    /// Asynchronously dereferences the cloud ref.
    member __.GetValue () = async {
        match cachedValue with
        | Some v -> return v
        | None ->
            let! v = getValue ()
            cachedValue <- Some v
            return v
    }

    /// Synchronously dereferences the cloud ref.
    member __.Value =
        match cachedValue with
        | Some v -> v
        | None ->
            let v = getValue () |> Async.RunSync
            cachedValue <- Some v
            v

    interface ICloudDisposable with
        member __.Dispose () = async {
            match source with
            | Table id -> return! getStore().TableStore.Value.Delete id
            | File(_, id) -> return! getStore().FileStore.DeleteFile id
        }

    static member internal Create(value : 'T, container : string, store : ICloudStore, serializer : ISerializer) = async {
        match store.TableStore with
        | Some ap when ap.IsSupportedValue value ->
            let! id = ap.Create value
            return new CloudRef<'T>(value, Table id, store)
        | _ ->
            let fileName = store.FileStore.CreateUniqueFileName container
            use! stream = store.FileStore.BeginWrite fileName
            do serializer.Serialize(stream, value)
            return new CloudRef<'T>(value, File(serializer.Id, fileName), store)
    }


namespace Nessos.MBrace.Store

open Nessos.MBrace

[<AutoOpen>]
module CloudRefUtils =

    type CloudStoreConfiguration with
        /// <summary>
        ///     Creates a new cloud ref
        /// </summary>
        /// <param name="value">Value for cloud ref.</param>
        /// <param name="container">FileStore container used for cloud ref. Defaults to configuration container.</param>
        /// <param name="serializer">Serialization used for object serialization. Default to configuration serializer.</param>
        member csc.CreateCloudRef(value : 'T, ?container : string, ?serializer) = 
            let serializer = defaultArg serializer csc.Serializer
            let container = defaultArg container csc.DefaultContainer
            CloudRef<'T>.Create(value, container, csc.Store, serializer)