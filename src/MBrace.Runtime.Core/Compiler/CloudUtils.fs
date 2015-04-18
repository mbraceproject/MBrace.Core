module internal MBrace.Runtime.Compiler.Utils

open System
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open MBrace.Core
open MBrace.Runtime.Utils.Reflection

/// checks if the given type or covariant type arguments are of type Cloud<'T>
let rec yieldsCloudBlock (t : Type) =
    if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Cloud<_>> then true else

    match t with
    | FSharpFunc(_,resultT) -> yieldsCloudBlock resultT
    | Named(_, genericArgs) -> Array.exists yieldsCloudBlock genericArgs
    | Param _ -> false
    | Array(t,_) -> yieldsCloudBlock t
    | Ptr(_,t) -> yieldsCloudBlock t
            
/// checks if given type is part of the MBrace.Core library
let isCloudPrimitive (t : Type) = t.Assembly = typeof<Cloud<_>>.Assembly

/// checks if given member contains CloudAttribute
let isLackingCloudAttribute (m : MemberInfo) =
    not (m.ContainsCustomAttributeRecursive<ReflectedDefinitionAttribute> () ||
            m.ContainsCustomAttributeRecursive<NoWarnAttribute> ())

/// matches against a `typeof` literal
let (|TypeOf|_|) (e : Expr) =
    match e with
    | SpecificCall <@ typeof<int> @> (_,types,_) when types.Length > 0 -> Some types.[0]
    | _ -> None

/// matches against a property whose return type contains cloud blocks
let (|CloudProperty|_|) (propInfo : PropertyInfo) =
    if yieldsCloudBlock propInfo.PropertyType && not <| isCloudPrimitive propInfo.DeclaringType then
        Some propInfo
    else None

/// matches against a method whose return type contains cloud blocks
let (|CloudMethod|_|) (methodInfo : MethodInfo) =
    let gmeth = if methodInfo.IsGenericMethod then methodInfo.GetGenericMethodDefinition() else methodInfo

    if yieldsCloudBlock gmeth.ReturnType && not <| isCloudPrimitive gmeth.DeclaringType then
        Some methodInfo
    else None

/// matches against a cloud method or property call
let (|CloudCall|_|) (e : Expr) =
    match e with
    | Call(_,CloudMethod m,_) -> Some(m :> MemberInfo, m)
    | PropertyGet(_, CloudProperty p,_) -> Some(p :> MemberInfo, p.GetGetMethod(true))
    | _ -> None

/// matches against a method or property call
let (|MemberInfo|_|) (e : Expr) =
    match e with
    | Call(_,m,_) -> Some (m :> MemberInfo, m.ReturnType)
    | PropertyGet(_,p,_) 
    | PropertySet(_,p,_,_) -> Some (p :> MemberInfo, p.PropertyType)
    | FieldGet(_,p) -> Some (p :> MemberInfo, p.FieldType)
    | FieldSet(_,p,_) -> Some (p :> MemberInfo, p.FieldType)
    | _ -> None

/// recognizes call to cloud builder
let (|CloudBuilder|_|) (name : string) (m : MethodInfo) =
    if m.DeclaringType = typeof<Builders.CloudBuilder> && m.Name = name then
        Some ()
    else
        None

/// recognizes a top-level 'cloud { }' expression
let (|CloudBuilderExpr|_|) (e : Expr) =
    match e with
    | Application(Lambda(bv,body),PropertyGet(None,_,[])) 
        when bv.Type = typeof<Builders.CloudBuilder> -> Some body
    | _ -> None

/// recognized monadic return
let (|CloudReturn|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "Return", [value]) -> Some value
    | _ -> None

/// recognizes monadic return!
let (|CloudReturnFrom|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "ReturnFrom", [expr]) -> Some expr
    | _ -> None

/// recognizes monadic bind
let (|CloudBind|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "Bind", [body ; Lambda(v, cont)]) -> 
        match cont with
        | Let(v', Var(v''), cont) when v = v'' -> Some(v', body, cont)
        | _ -> Some(v, body, cont)
    | _ -> None

/// recognizes monadic combine
let (|CloudCombine|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "Combine", [f ; g]) -> Some(f,g)
    | _ -> None

/// recognizes monadic delay
let (|CloudDelay|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "Delay", [Lambda(unitVar, body)]) when unitVar.Type = typeof<unit> -> Some body
    | _ -> None

/// recognizes monadic use bindings
let rec (|CloudUsing|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "Using", [_ ; Lambda(_, Let(v,body,cont)) ]) -> Some(v,body, cont)
    | CloudBind(_, b, Let(_,_,CloudUsing(v,_,cont))) -> Some(v,b,cont)
    | _ -> None

/// recognizes monadic try/with
let (|CloudTryWith|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "TryWith", [CloudDelay f ; Lambda(_, body)]) -> Some(f, body)
    | _ -> None

/// recognizes monadic try/finally
let (|CloudTryFinally|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "TryFinally", [CloudDelay f; ff]) -> Some(f, ff)
    | _ -> None

/// recognizes a monadic for loop
let (|CloudFor|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "For" , [inputs ; Lambda(_, Let(v,_,body)) ]) -> Some(inputs, v, body)
    | _ -> None

/// recognizes a monadic while loop
let (|CloudWhile|_|) (e : Expr) =
    match e with
    | Call(Some(Var _), CloudBuilder "While" , [cond ; CloudDelay f]) -> Some(cond, f)
    | _ -> None


/// gathers all quotation metadata from given expression
let getFunctionInfo (e : Expr) =
    let getFunctionInfo (source : Choice<MethodInfo, PropertyInfo>, e : Expr) =
        {
            Source = source
            Metadata = ExprMetadata.TryParse e |> Option.get
            Expr = e
            IsCloudExpression =
                match source with
                | Choice1Of2 m -> yieldsCloudBlock m.ReturnType
                | Choice2Of2 p -> yieldsCloudBlock p.PropertyType
        }

    Expr.getReflectedDefinitions e |> List.map getFunctionInfo


// gather the external bindings found in the body of an Expr<Cloud<'T>>
// these manifest potential closures that exist in distributed continutations
let gatherTopLevelCloudBindings (expr : Expr) =
    let rec aux ret gathered (exprs : Expr list) =
        match exprs with
        // expresions on rhs of sequential and let bindings are ignored
        // since they cannot be of monadic nature hence caught in the environment
        | Patterns.Sequential(_,e) :: rest -> aux ret gathered (e :: rest)
        | Let(v,_,cont) as e :: rest -> 
            let b = v, ExprMetadata.TryParse e
            aux ret (b :: gathered) (cont :: rest)

        // return values registered as dummy variables, but done only once
        // since all return statements must be of same type
        | CloudReturn e :: rest when not ret ->
            let b = new Var("return", e.Type, false), ExprMetadata.TryParse e
            aux true (b :: gathered) rest

        // gather variables from monadic bindings
        | CloudUsing(v,_,cont) as e :: rest -> 
            let b = v, ExprMetadata.TryParse e
            aux ret (b :: gathered) (cont :: rest)
        | CloudBind(v,_,cont) as e :: rest -> 
            let b = v, ExprMetadata.TryParse e
            aux ret (b :: gathered) (cont :: rest)

        // gather for loop index binding
        | CloudFor(_, idx, body) as e :: rest ->
            let b = idx, ExprMetadata.TryParse e
            aux ret (b :: gathered) (body :: rest)

        // remaining monadic constructs
        | CloudDelay f :: rest -> aux ret gathered (f :: rest)
        | CloudTryWith(f, h) :: rest -> aux ret gathered (f :: h :: rest)
        | CloudTryFinally(f, _) :: rest -> aux ret gathered (f :: rest)
        | CloudWhile(_,body) :: rest -> aux ret gathered (body :: rest)
        // traverse if-then-else statements
        | IfThenElse(_,a,b) :: rest -> aux ret gathered (a :: b :: rest)
        // ignore all other language constructs, cannot be of monadic nature
        | ShapeVar _ :: rest -> aux ret gathered rest
        | ShapeLambda _ :: rest -> aux ret gathered rest
        | ShapeCombination _ :: rest -> aux ret gathered rest
        // return state
        | [] -> List.rev gathered

    aux false [] [expr]