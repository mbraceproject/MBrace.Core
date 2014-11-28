namespace Nessos.MBrace.Store


type internal ResourceRegistry<'Resource> (getKey : 'Resource -> string) =
    let container = new System.Collections.Concurrent.ConcurrentDictionary<string, 'Resource> ()

    member __.Register(resource : 'Resource, ?force : bool) : unit =
        if defaultArg force false then
            container.AddOrUpdate(serializer.Id, serializer, fun _ _ -> serializer) |> ignore
        elif container.TryAdd(serializer.Id, serializer) then ()
        else
            let msg = sprintf "ResourceRegistry: a serializer with id '%O' already exists in registry." id
            invalidOp msg