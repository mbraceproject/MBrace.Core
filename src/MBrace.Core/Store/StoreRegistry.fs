namespace Nessos.MBrace.Store

open System.Collections.Concurrent

type private ResourceContainer<'Resource>(name : string, proj : 'Resource -> string) =
    let container = new ConcurrentDictionary<string, 'Resource> ()

    member __.Register(resource : 'Resource, ?force : bool) =
        let key = proj resource
        if defaultArg force false then
            container.AddOrUpdate(key, resource, fun _ _ -> resource) |> ignore
        elif container.TryAdd(key, resource) then ()
        else
            let msg = sprintf "StoreRegistry: %s with id '%O' already exists in registry." name key
            invalidOp msg

    member __.Resolve(id : string) =
        let mutable res = Unchecked.defaultof<'Resource>
        if container.TryGetValue(id, &res) then res
        else
            let msg = sprintf "StoreRegistry: no %s with id '%O' could be resolved." name id
            invalidOp msg

type StoreRegistry private () =
    static let serializers = new ResourceContainer<ISerializer> ("serializer", fun s -> s.Id)
    static let tableStores = new ResourceContainer<ICloudTableStore> ("table store", fun s -> s.UUID)
    static let fileStores = new ResourceContainer<ICloudFileStore> ("file store", fun s -> s.UUID)

    /// <summary>
    ///     Registers a serializer instance. 
    /// </summary>
    /// <param name="serializer">Serializer to be registered.</param>
    /// <param name="force">Force overwrite. Defaults to false.</param>
    static member Register(serializer : ISerializer, ?force : bool) : unit = serializers.Register(serializer, ?force = force)

    /// <summary>
    ///     Registers a file store instance. 
    /// </summary>
    /// <param name="fileStore">File store to be registered.</param>
    /// <param name="force">Force overwrite. Defaults to false.</param>
    static member Register(fileStore : ICloudFileStore, ?force : bool) : unit = fileStores.Register(fileStore, ?force = force)

    /// <summary>
    ///     Registers a table store instance. 
    /// </summary>
    /// <param name="tableStore">Table store to be registered.</param>
    /// <param name="force">Force overwrite. Defaults to false.</param>
    static member Register(tableStore : ICloudTableStore, ?force : bool) : unit = tableStores.Register(tableStore, ?force = force)

    /// <summary>
    ///     Resolves a registered serializer instance by id.
    /// </summary>
    /// <param name="id">Serializer id.</param>
    static member GetSerializer(id : string) = serializers.Resolve(id)

    /// <summary>
    ///     Resolves a registered FileStore instance by id.
    /// </summary>
    /// <param name="id">File store id.</param>
    static member GetFileStore(id : string) = fileStores.Resolve(id)

    /// <summary>
    ///     Resolves a registered TableStore instance by id.
    /// </summary>
    /// <param name="id">Table store id.</param>
    static member GetTableStore(id : string) = tableStores.Resolve(id)