namespace MBrace.Core.Internals

open System
open System.Runtime.Serialization

[<AutoOpen>]
module private ResourceRegistryUtils =

    // ResourceRegistry indexes by Type.AssemblyQualifiedName which is expensive to obtain.
    // Memoize for better resolution performance.
    let dict = new System.Collections.Concurrent.ConcurrentDictionary<Type, string>()
    let inline key<'T> : string = dict.GetOrAdd(typeof<'T>, fun t -> t.AssemblyQualifiedName)

/// Immutable dependency container used for pushing
/// runtime resources to the continuation monad.
[<Sealed ; DataContract>]
type ResourceRegistry private (index : Map<string, obj>) =

    static let empty = new ResourceRegistry(Map.empty)

    [<DataMember(Name = "Index")>]
    let index = index

    member private __.Index = index

    /// Gets all resources currently registered with factory.
    member __.InstalledResources = index |> Map.toArray |> Array.map fst

    /// <summary>
    ///     Creates a new resource registry by appending provided resource.
    ///     Any existing resources of the same type will be overwritten.
    /// </summary>
    /// <param name="resource">input resource.</param>
    member __.Register<'TResource>(resource : 'TResource) : ResourceRegistry = 
        new ResourceRegistry(Map.add key<'TResource> (box resource) index)

    /// <summary>
    ///     Creates a new resource registry by removing resource of given key.
    /// </summary>
    member __.Remove<'TResource> () : ResourceRegistry =
        new ResourceRegistry(Map.remove key<'TResource> index)

    /// Try Resolving resource of given type.
    member __.TryResolve<'TResource> () : 'TResource option = 
        match index.TryFind key<'TResource> with
        | Some boxedResource -> Some (unbox<'TResource> boxedResource)
        | None -> None

    /// Resolves resource of given type.
    member __.Resolve<'TResource> () : 'TResource =
        match index.TryFind key<'TResource> with
        | Some boxedResource -> unbox<'TResource> boxedResource
        | None -> 
            let msg = sprintf "Resource '%s' not installed in this context." typeof<'TResource>.Name
            raise <| ResourceNotFoundException msg

    /// Returns true iff registry instance contains resource of given type.
    member __.Contains<'TResource> () = index.ContainsKey key<'TResource>

    /// Gets the empty resource container.
    static member Empty = empty

    /// <summary>
    ///     Combines two resource registries into one.
    /// </summary>
    /// <param name="resources1">First resource registry.</param>
    /// <param name="resources2">Second resource registry.</param>
    static member Combine(resources1 : ResourceRegistry, resources2 : ResourceRegistry) =
        let mutable index = resources1.Index
        for kv in resources2.Index do
            index <- Map.add kv.Key kv.Value index
        new ResourceRegistry(index)

    /// <summary>
    ///     Combines two resource registries into one.
    /// </summary>
    /// <param name="resources">Resources to be combined.</param>
    static member Combine(resources : seq<ResourceRegistry>) =
        let mutable index = Map.empty
        for r in resources do
            for kv in r.Index do
                index <- Map.add kv.Key kv.Value index
        new ResourceRegistry(index)

/// Exception raised on missing resource resolution
and [<AutoSerializable(true)>] ResourceNotFoundException =
    inherit Exception
    internal new (message : string) = { inherit Exception(message) }
    private new (sI : SerializationInfo, sc : StreamingContext) =  { inherit Exception(sI, sc) }


/// Resource registry builder API
[<AutoOpen>]
module ResourceBuilder =

    type ResourceBuilder () =
        member __.Zero () = ResourceRegistry.Empty
        member __.Delay (f : unit -> ResourceRegistry) = f ()
        member __.Yield<'TResource> (resource : 'TResource) = ResourceRegistry.Empty.Register<'TResource> (resource)
        member __.YieldFrom (registry : ResourceRegistry) = registry
        member __.Combine(registry : ResourceRegistry, registry' : ResourceRegistry) = ResourceRegistry.Combine(registry, registry')

    /// resource registry builder
    let resource = new ResourceBuilder()