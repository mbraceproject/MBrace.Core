namespace Nessos.MBrace

//open System
//open System.IO
//
//open Nessos.MBrace.Store
//open Nessos.MBrace.Continuation
//
///// Represents an immutable reference to an
///// object that is persisted in the underlying store.
///// Cloud references are cached locally for performance.
//[<Sealed; AutoSerializable(true)>]
//type CloudRef<'T> private (init : 'T, file : CloudFile, serializer : ISerializer) =
//    
//    let serializerId = serializer.Id
//
//    // conveniently, the uninitialized value for optional fields coincides with 'None'.
//    // a more correct approach would initialize using an OnDeserialized callback
//    [<NonSerialized>]
//    let mutable cachedValue = Some init
//
//    /// Asynchronously dereferences the cloud ref.
//    member __.GetValue () = async {
//        match cachedValue with
//        | Some v -> return v
//        | None ->
//            let serializer = StoreRegistry.GetSerializer serializerId
//            let! v = file.Read(fun s -> async { return serializer.Deserialize<'T>(s) })
//            cachedValue <- Some v
//            return v
//    }
//
//    /// Synchronously dereferences the cloud ref.
//    member __.Value =
//        match cachedValue with
//        | Some v -> v
//        | None -> __.GetValue() |> Async.RunSync
//
//    interface ICloudDisposable with
//        member __.Dispose () = (file :> ICloudDisposable).Dispose()
//
//    static member internal Create(value : 'T, container : string, fileStore : ICloudFileStore, serializer : ISerializer) = 
//        async {
//                let fileName = fileStore.CreateUniqueFileName container
//                let! file = fileStore.CreateFile(fileName, fun stream -> async { return serializer.Serialize(stream, value) })
//                return new CloudRef<'T>(value, file, serializer)
//        }
//
//
//namespace Nessos.MBrace.Store
//
//open Nessos.MBrace
//
//[<AutoOpen>]
//module CloudRefUtils =
//
//    type ICloudFileStore with
//
//        /// <summary>
//        ///     Creates a new cloud ref
//        /// </summary>
//        /// <param name="value">Value for cloud ref.</param>
//        /// <param name="container">FileStore container used for cloud ref.</param>
//        /// <param name="serializer">Value serializer.</param>
//        member fs.CreateCloudRef(value : 'T, container : string, serializer : ISerializer) = 
//            CloudRef<'T>.Create(value, container, fs, serializer)