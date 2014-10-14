namespace Nessos.MBrace

    type ResourceResolver internal (index : Map<string, obj>) =
        interface IResourceResolver with
            member __.Resolve<'T> () = 
                match index.TryFind typeof<'T>.AssemblyQualifiedName with
                | None -> invalidOp <| sprintf "Resource '%s' not installed in this context." typeof<'T>.Name
                | Some o -> unbox<'T> o


    and ResourceResolverFactory private () =
        let index = ref Map.empty<string, obj>

        member __.AddResource<'T>(resource : 'T) = index := Map.add typeof<'T>.AssemblyQualifiedName (box resource) !index
        member __.GetResolver () = new ResourceResolver(!index) :> IResourceResolver
        member __.InstalledResources = !index |> Map.toSeq |> Seq.map fst |> Seq.toList

        static member Init () = new ResourceResolverFactory ()
        static member CreateEmptyResolver () = ResourceResolverFactory.Init().GetResolver()