﻿namespace MBrace.Continuation

open System
open System.Runtime.Serialization

[<AutoOpen>]
module private ResourceRegistryUtils =

    // ResourceRegistry indexes by Type.AssemblyQualifiedName which is expensive to obtain.
    // Memoize for better resolution performance.
    let dict = new System.Collections.Generic.Dictionary<Type, string>()
    let inline key (t : Type) : string =
        let mutable k = null
        if dict.TryGetValue(t, &k) then k
        else
            lock dict (fun () ->
                let k = t.AssemblyQualifiedName
                dict.[t] <- k
                k)

/// Immutable dependency container used for pushing
/// runtime resources to the continuation monad.
[<Sealed ; AutoSerializable(false)>]
type ResourceRegistry private (index : Map<string, obj>) =

    member private __.Index = index

    /// Gets all resources currently registered with factory.
    member __.InstalledResources = index |> Map.toArray |> Array.map fst

    /// <summary>
    ///     Creates a new resource registry by appending provided resource.
    ///     Any existing resources of the same type will be overwritten.
    /// </summary>
    /// <param name="resource">input resource.</param>
    member __.Register<'TResource>(resource : 'TResource) : ResourceRegistry = 
        __.Register(resource, typeof<'TResource>)

    /// <summary>
    ///     Creates a new resource registry by appending provided resource.
    ///     Any existing resources of the same type will be overwritten.
    /// </summary>
    /// <param name="resource">input resource.</param>
    /// <param name="ty">resource type.</param>
    member __.Register(resource : obj, ty : Type) : ResourceRegistry = 
        new ResourceRegistry(Map.add (key ty) (box resource) index)

    /// <summary>
    ///     Creates a new resource registry by removing resource of given key.
    /// </summary>
    member __.Remove<'TResource> () : ResourceRegistry =
        __.Remove(typeof<'TResource>)

    /// <summary>
    ///     Creates a new resource registry by removing resource of given key.
    /// </summary>
    /// <param name="ty">resource type.</param>
    member __.Remove(ty : Type) : ResourceRegistry =
        new ResourceRegistry(Map.remove (key ty) index)

    /// Try Resolving resource of given type
    member __.TryResolve<'TResource> () : 'TResource option = 
        match __.TryResolve(typeof<'TResource>) with
        | Some boxedResource -> Some (unbox<'TResource> boxedResource)
        | None -> None

    ///<summary> Try Resolving resource of given type.</summary>
    /// <param name="ty">resource type.</param>
    member __.TryResolve (ty : Type) : obj option = 
        match index.TryFind(key ty) with
        | Some boxedResource -> Some boxedResource
        | None -> None

    /// Resolves resource of given type
    member __.Resolve<'TResource> () : 'TResource =
        unbox<'TResource> <| __.Resolve(typeof<'TResource>) 

    /// <summary>Resolves resource of given type</summary>
    /// <param name="ty">resource type.</param>
    member __.Resolve (ty : Type) : obj =
        match index.TryFind (key ty) with
        | Some boxedResource -> boxedResource
        | None -> 
            let msg = sprintf "Resource '%s' not installed in this context." ty.Name
            raise <| ResourceNotFoundException msg

    /// Returns true iff registry instance contains resource of given type
    member __.Contains<'TResource> () = __.Contains(typeof<'TResource>)

    /// Returns true iff registry instance contains resource of given type
    /// <param name="ty">resource type.</param>
    member __.Contains (ty : Type) = index.ContainsKey(key ty)


    /// Creates an empty resource container
    static member Empty = new ResourceRegistry(Map.empty)

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