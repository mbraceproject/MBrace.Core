namespace Nessos.MBrace.Runtime

[<AutoOpen>]
module private ResourceUtils =
        
    let inline key<'T> = typeof<'T>.AssemblyQualifiedName

/// Exception raised on missing resource resolution
type ResourceNotFoundException internal (message : string) = 
    inherit System.Exception(message)

/// Immutable dependency container used for pushing
/// runtime resources to the continuation monad.
[<Sealed ; AutoSerializable(false)>]
type ResourceRegistry private (index : Map<string, obj>) =

    /// Try Resolving resource of given type
    member __.TryResolve<'TResource> () = index.TryFind key<'TResource> |> Option.map unbox<'TResource>

    /// Resolves resource of given type
    member __.Resolve<'TResource> () =
        match index.TryFind key<'TResource> with
        | Some boxedResource -> unbox<'TResource> boxedResource
        | None -> raise <| ResourceNotFoundException (sprintf "Resource '%s' not installed in this context." typeof<'TResource>.Name)

    /// Creates an empty resource container
    static member Empty = new ResourceRegistry(Map.empty)

    /// <summary>
    ///     Creates a new resource registry by appending provided resource.
    ///     Any existing resources of the same type will be overwritten.
    /// </summary>
    /// <param name="resource">input resource.</param>
    member __.Register<'TResource>(resource : 'TResource) = new ResourceRegistry(Map.add key<'TResource> (box resource) index)

    member private __.Index = index
    member internal __.CombineWith(other : ResourceRegistry) = 
        let mutable index = index
        for kv in other.Index do
            index <- Map.add kv.Key kv.Value index
        new ResourceRegistry(index)

    /// Gets all resources currently registered with factory.
    member __.InstalledResources = index |> Map.toArray |> Array.map fst


/// Resource registry builder API
[<AutoOpen>]
module ResourceBuilder =

    type ResourceBuilder () =
        member __.Delay (f : unit -> ResourceRegistry) = f ()
        member __.Yield<'TResource> (resource : 'TResource) = ResourceRegistry.Empty.Register<'TResource> (resource)
        member __.YieldFrom (registry : ResourceRegistry) = registry
        member __.Combine(registry : ResourceRegistry, registry' : ResourceRegistry) = registry.CombineWith registry'

    /// resource registry builder
    let resource = new ResourceBuilder()