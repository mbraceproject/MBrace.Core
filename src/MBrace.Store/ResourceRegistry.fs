namespace Nessos.MBrace.Store

open System.Collections.Concurrent

type IResource =
    /// Unique identifier for cloud file provider,
    /// such as a server endpoint or connection string.
    abstract UUID : string

type internal ResourceRegistry<'TResource when 'TResource :> IResource> private () =
    static let index = new ConcurrentDictionary<string, 'TResource> ()
    static member GetId (resource : 'TResource) = resource.UUID
    static member Register(resource : 'TResource) = 
        index.AddOrUpdate(ResourceRegistry<'TResource>.GetId resource, resource, fun _ _ -> resource)

    static member Resolve(id : string) =
        let mutable provider = Unchecked.defaultof<_>
        let ok = index.TryGetValue(id, &provider)
        if ok then provider
        else
            invalidOp "could not recover resource %s." id

and ResourceRegistry =
    static member Register<'TResource when 'TResource :> IResource> (resource : 'TResource) =
        ResourceRegistry<'TResource>.Register(resource)