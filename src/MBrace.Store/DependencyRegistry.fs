namespace Nessos.MBrace.Store

open System.Collections.Concurrent

type IResource =
    /// Unique identifier for cloud file provider,
    /// such as implementation name or server endpoint.
    abstract UUID : string

type private DependencyContainer<'TResource when 'TResource :> IResource> private () =
    static let index = new ConcurrentDictionary<string, 'TResource> ()
    static member inline GetId (resource : 'TResource) = resource.UUID
    static member Register(resource : 'TResource) = 
        index.AddOrUpdate(DependencyContainer<'TResource>.GetId resource, resource, fun _ _ -> resource)

    static member Resolve(id : string) =
        let mutable provider = Unchecked.defaultof<_>
        let ok = index.TryGetValue(id, &provider)
        if ok then provider
        else
            invalidOp "could not recover resource %s." id

and Dependency =
    static member Register<'TResource when 'TResource :> IResource> (resource : 'TResource) =
        DependencyContainer<'TResource>.Register(resource)

    static member Resolve<'TResource when 'TResource :> IResource> (id : string) =
        DependencyContainer<'TResource>.Resolve(id)

    static member GetId<'TResource when 'TResource :> IResource> (resource : 'TResource) = 
        DependencyContainer<'TResource>.GetId(resource)