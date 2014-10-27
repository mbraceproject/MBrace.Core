namespace Nessos.MBrace.Runtime.Compiler

open System
open System.Reflection

open Microsoft.FSharp.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.ExprShape

open Nessos.FsPickler
open Nessos.Vagrant

open Nessos.MBrace

open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Utils
open Nessos.MBrace.Runtime.Utils.Reflection
open Nessos.MBrace.Runtime.Utils.PrettyPrinters
open Nessos.MBrace.Runtime.Compiler.Utils

[<AutoOpen>]
module private CompilerImpl =

    type CompilerInfo =
        | Warning of string
        | Error of string
    with
        member i.Message = match i with Warning m -> m | Error m -> m

    /// specifies if given MemberInfo is prohibited for use within cloud workflows
    let isProhibitedMember (m : MemberInfo) =
        if m = null then false else
        let assembly = match m with :? Type as t -> t.Assembly | m -> m.DeclaringType.Assembly
        let assemblyName = assembly.GetName()

        match assemblyName.Name with
        | "MBrace.Runtime"
        | "MBrace.Runtime.Base"
        | "MBrace.Client" -> true
        | _ -> false

    let isProhibitedType (t : Type) = Type.traverse t |> Seq.exists isProhibitedMember

    let printLocation (metadata : ExprMetadata) =
        sprintf "%s(%d,%d)" metadata.File metadata.StartRow metadata.StartCol

    let printMemberInfo (m : MemberInfo) = sprintf "%s.%s" m.DeclaringType.Name m.Name

    let thisAssembly = Assembly.GetExecutingAssembly()

    // checks given expression tree, generating mbrace-related errors and warnings:
    // 1. checks if calls to cloud blocks are accompanied with a [<Cloud>] attribute
    // 2. checks if top-level bindings in the cloud monad are serializable
    // 3. checks that cloud block methods are static
    // 4. checks that cloud blocks do not make calls to the mbrace client API.
    // 5. checks if bindings to cloud expressions are closures (i.e. Expr.Value leaves)
    let checkExpression (name : string option) (metadata : ExprMetadata option) (expr : Expr) =

        let gathered = ref []

        let blockName =
            match name with
            | None -> "block"
            | Some name -> sprintf "block '%s'" name

        let log errorType (node : Expr) fmt =
            let prefix =
                // try parse current node for metadata, fall back to top-level metadata if not found
                match ExprMetadata.TryParse node, metadata with
                | Some m, _ 
                | None, Some m -> sprintf "%s: " <| printLocation m
                | None, None -> ""

            Printf.ksprintf(fun msg -> gathered := errorType (sprintf "%s%s" prefix msg) :: gathered.Value) fmt

        let checkCurrentNode (e : Expr) =
            match e with
            // cloud { ... }
            | CloudBuilderExpr body ->
                let bindings = gatherTopLevelCloudBindings body
                for v, metadata in bindings do
                    if not <| FsPickler.IsSerializableType v.Type then
                        log Error e "%s has binding '%s' of type '%s' that is not serializable." blockName v.Name <| Type.prettyPrint v.Type
                
            // let! x = cloudExpr
            | CloudCall (memberInfo, methodBase) ->
                // fail if cloud expression is not static method
                if not methodBase.IsStatic then
                    log Error e "%s references non-static cloud workflow '%s'. This is not supported." blockName <| printMemberInfo memberInfo

                // referenced cloud expression is not a reflected definition
                elif isLackingCloudAttribute memberInfo then
                    log Warning e "%s depends on '%s' which lacks [<Cloud>] attribute." blockName <| printMemberInfo memberInfo

            // cloud block loaded from a field; unlikely but possible
            | FieldGet(_,f) when yieldsCloudBlock f.FieldType ->
                // fail if cloud expression is not static method
                if not f.IsStatic then
                    log Error e "%s references non-static cloud workflow '%s'. This is not supported." blockName <| printMemberInfo f

                elif isLackingCloudAttribute f then
                    log Warning e "%s depends on cloud workflow '%s' which lacks [<Cloud>] attribute." blockName f.Name

            // cloud block loaded a closure;
            // can happen in cases where cloud blocks are defined in nested let bindings:
            // e.g. let test () = let wf () = cloud { return 42 } in <@ wf () @>
            // this is a common mistake, so need to make sure that error message is well-documented
            | Value(o,t) when yieldsCloudBlock t ->
                // closure is a function ; can extract a name
                if FSharpType.IsFunction t && o <> null then
                    let name = o.GetType().Name.Split('@').[0]
                    log Warning e "%s references closure '%s'. All cloud blocks should be top-level let bindings." blockName name

                // unknown container : not much can be reported here
                else
                    log Warning e "%s references a closure. All cloud blocks should be top-level let bindings." blockName
                
            // typeof<_> literal
            | TypeOf t when isProhibitedType t ->
                log Error e "%s references prohibited type '%s'." blockName <| Type.prettyPrint t
                    
            // generic Call/PropertyGet/PropertySet
            | MemberInfo (m,returnType) ->
                // check if cloud expression references inappropriate MBrace libraries
                if isProhibitedMember m then
                    log Error e "%s references prohibited member '%s'." blockName <| printMemberInfo m

                elif isProhibitedType returnType then
                    log Error e "%s references member '%s' of prohibited type '%s'." blockName <| printMemberInfo m <| Type.prettyPrint returnType

            // values captured in closure
            | Value(_,t) when isProhibitedType t -> log Error e "%s references prohibited type '%s'." blockName <| Type.prettyPrint t
            | _ -> ()


        Expr.iter checkCurrentNode expr

        gathered.Value


    let checkTopLevelQuotation (expr : Expr) =
        let metadata = ExprMetadata.TryParse expr
        checkExpression None metadata expr

    let checkFunctionInfo (f : FunctionInfo) =
        checkExpression (Some f.FunctionName) (Some f.Metadata) f.Expr

    let rec tryGetName (expr : Expr) =
        match expr with 
        | MemberInfo (m,_) -> Some m.Name
        | ShapeLambda(_,body) -> tryGetName body
        | _ -> None

    /// the main compiler method
    let compile name (expr : Expr<Cloud<'T>>) =

        let name = 
            match name with
            | Some name -> name
            | None -> defaultArg (tryGetName expr) ""
            
        // gather function info
        let functions = getFunctionInfo expr

        let errors, warnings =
            seq {
                yield! checkTopLevelQuotation expr

                for f in functions do yield! checkFunctionInfo f
            } 
            |> Seq.distinct 
            |> Seq.toList
            |> List.partition (function Error _ -> true | Warning _ -> false)

        let warnings = warnings |> List.map (fun e -> e.Message)

        match errors with
        | _ :: _ ->
            let errors = errors |> List.map (fun e -> e.Message)
            raise <| new CompilerException(name, typeof<'T>, errors, warnings)

        | [] -> name, functions, warnings


/// Provides methods for static checks and metadata gathering on cloud computations.
type CloudCompiler =

    /// <summary>
    ///     Compiles a quoted cloud computation into a cloud computation package.
    /// </summary>
    /// <param name="expr">Quoted cloud computation.</param>
    /// <param name="name">Optional computation name.</param>
    static member Compile(expr : Expr<Cloud<'T>>, ?name : string) : CloudComputation<'T> =
        let dependencies = VagrantRegistry.ComputeObjectDependencies expr
        let name, functions, warnings = compile name expr
        new QuotedCloudComputation<'T>(name, expr, warnings, dependencies, functions) :> CloudComputation<'T>

    /// <summary>
    ///     Compiles a cloud computation into a cloud computation package.
    /// </summary>
    /// <param name="workflow">Cloud workflow.</param>
    /// <param name="name">Optional computation name.</param>
    static member Compile(workflow : Cloud<'T>, ?name : string) =
        let name = defaultArg name ""
        let dependencies = VagrantRegistry.ComputeObjectDependencies workflow
        new BareCloudComputation<'T>(name, workflow, [], dependencies) :> CloudComputation<'T>