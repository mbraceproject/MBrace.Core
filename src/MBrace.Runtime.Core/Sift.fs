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
    | Array of parent:Array * index:uint64
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

let private ofSmallIndices (bounds:int[]) (indices:int[]) =
    let mutable gI = 0uL
    for i = 0 to bounds.Length - 1 do
        gI <- uint64 bounds.[i] * gI + uint64 indices.[i]
    gI

let private toSmallIndices (bounds:int[]) (index:uint64) =
    let mutable index = index
    let indices = Array.zeroCreate<int> bounds.Length
    for i = bounds.Length - 1 downto 0 do
        let b = uint64 bounds.[i]
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
                | Array(parent = p; index = is) -> p.SetValue(null, is)

            | _ when isFirst ->
                match obj with
                | :? Array as array ->
                    if array.GetType().GetElementType().IsValueType then ()
                    elif array.Rank <= 1 then
                        for i = 0L to array.LongLength - 1L do
                            let e = array.GetValue(i)
                            traverse (Some (Array(array, i))) e
                    else
                        let dimensions = [| for i in 0 .. array.Rank - 1 -> array.GetLength i |]
                        for i = 0L to array.LongLength - 1L do
                            let e = array.GetValue(toSmallIndices dimensions i)
                            traverse (Some (Array(array, i))) e
                | _ ->
                    let t = obj.GetType()
                    for f in gatherFields t do
                        let value = f.GetValue(obj)
                        traverse (Some (Field(obj, f))) value

        | _ -> ()

    do traverse None obj
                    
                
                
            
//        | :? Array as array ->
//            array.Set
//            
//       
//let xs = Array3D.init 10 10 10 (fun i j k -> (i,j,k)) :> System.Array
//
//([|1..100|]).GetValue [|0;1;1|]
//
//let foo = xs.GetLongLength (4)



//let gatherObjs (o : obj) =
//    let gen = new ObjectIDGenerator()
//
//    let rec traverse (gathered : obj list) : obj list -> obj list =
//        function
//        | [] -> gathered
//        | o :: rest when o = null -> traverse gathered rest
//        | o :: rest ->
//            let firstTime = ref false
//            gen.GetId(o, firstTime) |> ignore
//            if firstTime.Value then
//                let t = o.GetType()
//                let nested =
//                    if t.IsValueType then []
//                    elif t.IsArray then
//                        [ for e in (o :?> Array) -> e ]
//                    else
//                        let fields = t.GetFields(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
//                        [ for fInfo in fields -> fInfo.GetValue o ]
//
//                traverse (o :: gathered) (nested @ rest)
//
//            else traverse gathered rest
//
//    traverse [] [o]


//let gatherTypes : obj -> _ =
//    gatherObjs
//    >> Seq.map (fun o -> o.GetType())
//    >> Seq.distinctBy (fun t -> t.AssemblyQualifiedName)
//    >> Seq.toList