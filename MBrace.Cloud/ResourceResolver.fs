namespace Nessos.MBrace

    /// Exception raised on missing resource resolution
    exception ResourceNotFoundException of string
     with
        override e.Message = e.Data0

    [<AutoSerializable(false)>]
    type internal ResourceResolver internal (index : Map<string, obj>) =
        interface IResourceResolver with
            member __.Resolve<'T> () = 
                match index.TryFind typeof<'T>.AssemblyQualifiedName with
                | None -> raise <| ResourceNotFoundException (sprintf "Resource '%s' not installed in this context." typeof<'T>.Name)
                | Some o -> unbox<'T> o

    /// Factory type for defining resource resolution contexts
    type ResourceResolverFactory private () =
        let index = ref Map.empty<string, obj>

        /// Add a new resource of type T; this will overwrite any existing instances of similar type
        member __.Register<'T>(resource : 'T) = index := Map.add typeof<'T>.AssemblyQualifiedName (box resource) !index
        /// Generates an immutable IResourceResolver with given registrations
        member __.GetResolver () = new ResourceResolver(!index) :> IResourceResolver
        /// Gets all resources currently registered with factory.
        member __.InstalledResources = !index |> Map.toSeq |> Seq.map fst |> Seq.toList

        /// Initializes an empty resource resolver factory
        static member Init () = new ResourceResolverFactory ()
        /// Creates an empty IResourceResolver instance.
        static member CreateEmptyResolver () = ResourceResolverFactory.Init().GetResolver()