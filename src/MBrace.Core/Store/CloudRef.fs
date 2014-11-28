namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

type private CloudRefStorageSource<'T> =
    | Table of CloudAtom<'T>
    | File of serializerId:string * CloudFile
with
    member src.GetValue () = async {
        match src with
        | Table atom -> return! atom.GetValue()
        | File(serializerId, file) ->
            let serializer = StoreRegistry.GetSerializer serializerId
            use! stream = file.BeginRead()
            return serializer.Deserialize<'T>(stream)
    }

    member src.Disposable =
        match src with
        | Table atom -> atom :> ICloudDisposable
        | File(_,file) -> file :> ICloudDisposable


/// Represents an immutable reference to an
/// object that is persisted in the underlying store.
/// Cloud references are cached locally for performance.
[<Sealed; AutoSerializable(true)>]
type CloudRef<'T> private (init : 'T, source : CloudRefStorageSource<'T>) =

    // conveniently, the uninitialized value for optional fields coincides with 'None'.
    // a more correct approach would initialize using an OnDeserialized callback
    [<NonSerialized>]
    let mutable cachedValue = Some init

    /// Asynchronously dereferences the cloud ref.
    member __.GetValue () = async {
        match cachedValue with
        | Some v -> return v
        | None ->
            let! v = source.GetValue()
            cachedValue <- Some v
            return v
    }

    /// Synchronously dereferences the cloud ref.
    member __.Value =
        match cachedValue with
        | Some v -> v
        | None ->
            let v = source.GetValue() |> Async.RunSync
            cachedValue <- Some v
            v

    interface ICloudDisposable with
        member __.Dispose () = source.Disposable.Dispose()

    static member internal Create(value : 'T, container : string, fileStore : ICloudFileStore, 
                                        serializer : ISerializer, ?tableStore : ICloudTableStore) = async {
        match tableStore with
        | Some ap when ap.IsSupportedValue value ->
            let! atom = ap.CreateAtom value
            return new CloudRef<'T>(value, Table atom)
        | _ ->
            let fileName = fileStore.CreateUniqueFileName container
            let! file = fileStore.CreateFile(fileName, fun stream -> async { return serializer.Serialize(stream, value) })
            return new CloudRef<'T>(value, File(serializer.Id, file))
    }


namespace Nessos.MBrace.Store

open Nessos.MBrace

[<AutoOpen>]
module CloudRefUtils =

    type CloudRef =

        /// <summary>
        ///     Creates a new cloud ref
        /// </summary>
        /// <param name="value">Value for cloud ref.</param>
        /// <param name="container">FileStore container used for cloud ref. Defaults to configuration container.</param>
        /// <param name="serializer">Serialization used for object serialization. Default to configuration serializer.</param>
        static member CreateCloudRef(value : 'T, container : string, fileStore, serializer, ?tableStore) = 
            CloudRef<'T>.Create(value, container, fileStore, serializer, ?tableStore = tableStore)