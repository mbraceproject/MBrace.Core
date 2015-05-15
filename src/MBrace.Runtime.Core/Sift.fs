module MBrace.Runtime.Utils.Sift

open System
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Generic

type SiftAction =
    | Sift
    | Resume
    | Ignore

type ParentContext =
    | Field of parent:obj * field:FieldInfo
    | Array of parent:Array * index:int
    | ArrayMultiDimensional of parent:Array * indices:int[]

type SiftContext =
    {
        Graph : obj
        Holes : (int64 * ParentContext) []
    }

type SiftValue =
    {
        Id : int64
        Value : obj
    }

let  getDimensions (array:Array) =
    [| for i in 0 .. array.Rank - 1 -> array.GetLength i |]

let private toSmallIndices (bounds:int[]) (index:int64) =
    let mutable index = index
    let indices = Array.zeroCreate<int> bounds.Length
    for i = bounds.Length - 1 downto 0 do
        let b = int64 bounds.[i]
        indices.[i] <- int (index % b)
        index <- index / b
    indices

let private gatherFields (t : Type) =
    // resolve conflicts, index by declaring type and field name
    let gathered = new Dictionary<Type * string, FieldInfo> ()

    let instanceFlags = 
        BindingFlags.NonPublic ||| BindingFlags.Public ||| 
        BindingFlags.Instance ||| BindingFlags.FlattenHierarchy

    let rec gather (t : Type) =
        let fields = t.GetFields(instanceFlags)
        for f in fields do
            if not f.FieldType.IsValueType then
                let k = f.DeclaringType, f.Name
                if not <| gathered.ContainsKey k then
                    gathered.Add(k, f)

        match t.BaseType with
        | null -> ()
        | t when t = typeof<obj> -> ()
        | bt -> gather bt

    do gather t

    gathered |> Seq.map (fun kv -> kv.Value) |> Seq.toArray

let sift (selector : obj -> SiftAction) (obj:obj) =
    let idGen = new ObjectIDGenerator()
    let sifted = new Dictionary<int64, ParentContext * obj>()

    let rec traverse (parent:ParentContext option) (obj:obj) =
        match obj with
        | null -> ()
        | :? MemberInfo -> ()
        | :? Assembly -> ()
        | :? Delegate as d ->
            let _, isFirst = idGen.GetId obj
            if isFirst then
                let linked = 
                    match d.GetInvocationList() with
                    | [|_|] -> [| d |]
                    | ds -> ds

                for d in linked do
                    if not d.Method.IsStatic then
                        traverse None d.Target

        | _ ->
            let id, isFirst = idGen.GetId obj
            match selector obj, parent with
            | Ignore, _ -> ()
            | Sift, Some ctx ->
                if isFirst then sifted.Add(id, (ctx, obj))
                match ctx with
                | Field(parent = p; field = f) -> f.SetValue(p, null)
                | Array(parent = p; index = i) -> p.SetValue(null, i)
                | ArrayMultiDimensional(parent = p; indices = is) -> p.SetValue(null, is)

            | _ when isFirst ->
                match obj with
                | :? Array as array ->
                    if array.GetType().GetElementType().IsValueType then ()
                    elif array.Rank <= 1 then
                        for i = 0 to array.Length - 1 do
                            let e = array.GetValue(i)
                            traverse (Some (Array(array, i))) e
                    else
                        let dimensions = [| for i in 0 .. array.Rank - 1 -> array.GetLength i |]
                        for i in 0L .. array.LongLength - 1L do
                            let indices = toSmallIndices dimensions i
                            let e = array.GetValue indices
                            traverse (Some (ArrayMultiDimensional(array, indices))) e
                | _ ->
                    let t = obj.GetType()
                    for f in gatherFields t do
                        let value = f.GetValue(obj)
                        traverse (Some (Field(obj, f))) value

            | _ -> ()

    do traverse None obj

    let holes, siftedValues =
        sifted 
        |> Seq.map (function KeyValue(id, (ctx, obj)) -> (id, ctx), { Id = id ; Value = obj })
        |> Seq.toArray
        |> Array.unzip

    { Graph = obj; Holes = holes }, siftedValues

        
let unsift (ctx : SiftContext) (values : SiftValue []) =
    let map = values |> Seq.map (fun sv -> sv.Id, sv.Value) |> Map.ofSeq
    let data = ctx.Holes |> Array.map (fun (id, ctx) -> ctx, map.[id])
    for ctx, value in data do
        match ctx with
        | Field(p, f) -> f.SetValue(p, value)
        | Array(p, i) -> p.SetValue(value, i)
        | ArrayMultiDimensional(p, is) -> p.SetValue(p, is)

    ctx.Graph


let graph = (Some (12, [|1 .. 10000|]), [1;2;3], Some("hello", [|"bye"|]))

let ctx, values = sift (function :? Array -> Sift | _ -> Resume) graph