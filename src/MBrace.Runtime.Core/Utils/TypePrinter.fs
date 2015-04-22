namespace MBrace.Runtime.Utils.PrettyPrinters

open System
open System.Collections.Generic
open System.Text.RegularExpressions

open MBrace.Runtime.Utils.Reflection

[<AutoOpen>]
module private Utils =

    type Priority =
        | Generic = 5
        | Postfix = 4
        | Tuple = 3
        | Arrow = 2
        | Const = 1
        | Bottom = 0

    let primitives =
        dict [
            typeof<unit>, "unit"
            typeof<bool>, "bool"
            typeof<obj>, "obj"
            typeof<exn>, "exn"
            typeof<char>, "char"
            typeof<byte>, "byte"
            typeof<decimal>, "decimal"
            typeof<string>, "string"
            typeof<float>, "float"
            typeof<single>, "single"
            typeof<sbyte>, "sbyte"
            typeof<int16>, "int16"
            typeof<int32>, "int"
            typeof<int64>, "int64"
            typeof<uint16>, "uint16"
            typeof<uint32>, "uint32"
            typeof<uint64>, "uint64"
        ]

    let postfixGenerics =
        dict [
            typedefof<_ list>, "list"
            typedefof<_ ref>, "ref"
            typedefof<_ option>, "option"
            typedefof<_ seq>, "seq"
        ]

    type IDictionary<'K,'V> with
        member d.TryFind k =
            let ok,v = d.TryGetValue k
            if ok then Some v
            else None

    let (|Primitive|_|) (t : Type) = primitives.TryFind t
    let (|PostfixGeneric|_|) (t : Type) = 
        match t with
        | Named(t, [|param|]) -> 
            match postfixGenerics.TryFind t with
            | Some postfix -> Some(param, postfix)
            | None -> None
        | _ -> None

    let private fsharpPrefix = Regex("^FSharp")
    let getTypeName (t : Type) =
        let name =
            if t.Namespace <> null && t.Namespace.StartsWith("Microsoft.FSharp") then // Namespace in C# anonymous types is null
                fsharpPrefix.Replace(t.Name, "")
            else t.Name
                
        name.Split('`').[0]

[<RequireQualifiedAccess>]
module Type =

    /// <summary>
    ///   Pretty print a type name.  
    /// </summary>
    /// <param name="t">Type to be printed.</param>
    let prettyPrint (t : Type) =
       
        let rec traverse context (t : Type) =
            seq {
                match t with
                | Primitive name -> yield name
                | Named (t, [||]) -> yield t.FullName
                | Array (arg, rk) -> 
                    yield! traverse Priority.Postfix arg
                    match rk with
                    | None -> yield " []"
                    | Some 1 -> yield " [*]"
                    | Some rk ->
                        yield "["
                        for _ in 1 .. rk - 1 -> ","
                        yield "]"

                | Ptr (true,arg) -> yield! traverse Priority.Postfix arg ; yield "*"
                | Ptr (false,arg) -> yield! traverse Priority.Postfix arg ; yield "&"
                | Param(t,_) -> yield "'" ; yield t.Name

                // Generic types
                | FsTuple args ->
                    let wrap = context >= Priority.Tuple

                    if wrap then yield "("

                    yield! traverse Priority.Tuple args.[0]

                    for arg in args.[1..] do
                        yield " * "
                        yield! traverse Priority.Tuple arg

                    if wrap then yield ")"

                | FSharpFunc (args, rt) ->
                    let wrap = context >= Priority.Arrow
                    
                    if wrap then yield "("
                        
                    for arg in args do
                        yield! traverse Priority.Arrow arg
                        yield " -> "
                    yield! traverse Priority.Arrow rt

                    if wrap then yield ")"

                | PostfixGeneric(t, postfix) ->
                    yield! traverse Priority.Postfix t ; yield " " ; yield postfix

                | Named (t0, args) ->
                    yield getTypeName t0
                    yield "<"
                    yield! traverse Priority.Generic args.[0]

                    for arg in args.[1..] do
                        yield ","
                        yield! traverse Priority.Generic arg

                    yield ">"
            }

        traverse Priority.Bottom t |> String.concat ""