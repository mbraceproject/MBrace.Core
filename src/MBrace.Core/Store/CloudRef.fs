namespace Nessos.MBrace

open System
open System.IO

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime

type private CloudRefStorageSource<'T> =
    | Entry of CloudAtom<'T>
    | File of serializerId:string * CloudFile
with
    member s.ReadValue () = async {
        match s with
        | Entry a -> return! a.GetValue()
        | File(serializerId, file) ->
            let serializer = Dependency.Resolve<ISerializer> serializerId
            use! stream = file.BeginRead()
            return serializer.Deserialize<'T>(stream)
    }

    member s.Disposable =
        match s with
        | Entry a -> a :> ICloudDisposable
        | File(_,f) -> f :> ICloudDisposable

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
            let! v = source.ReadValue()
            cachedValue <- Some v
            return v
    }

    /// Synchronously dereferences the cloud ref.
    member __.Value =
        match cachedValue with
        | Some v -> v
        | None ->
            let v = source.ReadValue() |> Async.RunSync
            cachedValue <- Some v
            v

    interface ICloudDisposable with
        member __.Dispose () = source.Disposable.Dispose()

    static member internal Create(value : 'T, config : CloudStoreConfiguration) = async {
        match config.AtomProvider with
        | Some ap when ap.IsSupportedValue value ->
            let! atom = ap.CreateAtom value
            return new CloudRef<'T>(value, Entry atom)
        | _ ->
            let fileName = config.FileProvider.CreateUniqueFileName "TODO : implement process-bound container name"
            let! file = config.FileProvider.CreateFile(fileName, fun stream -> async { return config.Serializer.Serialize(stream, value) })
            return new CloudRef<'T>(value, File(config.Serializer.UUID, file))
    }