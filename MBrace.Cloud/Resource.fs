namespace Nessos.MBrace.Runtime
    
    open Nessos.MBrace

    [<AutoOpen>]
    module private ResourceUtils =
        
        let inline key<'T> = typeof<'T>.AssemblyQualifiedName

    /// Cloud resource runtime dependency resolver
    type ResourceRegistry private (index : Map<string, obj>) =

        /// Try Resolving resource of given type
        member __.TryResolve<'TResource> () = index.TryFind key<'TResource> |> Option.map unbox<'TResource>

        /// Resolves resource of given type
        member __.Resolve<'TResource> () =
            match index.TryFind key<'TResource> with
            | Some boxedResource -> unbox<'TResource> boxedResource
            | None -> raise <| ResourceNotFoundException (sprintf "Resource '%s' not installed in this context." typeof<'TResource>.Name)

        /// Creates an empty resource resolver
        static member Empty = new ResourceRegistry(Map.empty)

        /// Creates a new Resolver factory with an appended resource
        member __.Register<'TResource>(resource : 'TResource) = new ResourceRegistry(Map.add key<'TResource> (box resource) index)

        member private __.Index = index
        member internal __.CombineWith(other : ResourceRegistry) = 
            let mutable index = index
            for kv in other.Index do
                index <- Map.add kv.Key kv.Value index
            new ResourceRegistry(index)

        /// Gets all resources currently registered with factory.
        member __.InstalledResources = index |> Map.toArray |> Array.map fst


    /// Resource builder API
    [<AutoOpen>]
    module ResourceBuilder =

        type ResourceBuilder () =
            member __.Delay (f : unit -> ResourceRegistry) = f ()
            member __.Yield<'TResource> (resource : 'TResource) = ResourceRegistry.Empty.Register<'TResource> (resource)
            member __.YieldFrom (registry : ResourceRegistry) = registry
            member __.Combine(registry : ResourceRegistry, registry' : ResourceRegistry) = registry.CombineWith registry'

        let resource = new ResourceBuilder()