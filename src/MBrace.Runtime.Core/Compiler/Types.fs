namespace MBrace.Runtime.Compiler

open System
open System.Reflection
open System.Runtime.Serialization

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

open Nessos.Vagabond
open Nessos.FsPickler

open MBrace
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Serialization

/// Parsed version of Expr.CustomAttributes
type ExprMetadata =
    {
        File : string
        StartRow : int ; StartCol : int
        EndRow   : int ; EndCol : int
    }
with
    /// <summary>
    ///     Parses an expression for its .CustomAttribute data. Returns None if not specified.
    /// </summary>
    /// <param name="expr">Input quotation.</param>
    static member TryParse(expr : Expr) =
        match expr.CustomAttributes with
        | [ NewTuple [_; NewTuple [Value (file, _); Value (srow, _); Value (scol, _); Value (erow, _); Value(ecol, _)]] ] -> 
            Some { 
                File = file :?> string

                StartRow = srow :?> int ;   StartCol = scol :?> int
                EndRow   = erow :?> int ;   EndCol = ecol :?> int    
            }
        | _ -> None

/// Reflected method/property information.
type FunctionInfo = 
    {
        /// Source metadata.
        Source : Choice<MethodInfo, PropertyInfo>
        /// Expr custom attributes.
        Metadata : ExprMetadata
        /// Reflected definition.
        Expr : Quotations.Expr
        /// Is quoted cloud workflow.
        IsCloudExpression : bool
    }
with
    member fi.FunctionName =
        match fi.Source with
        | Choice1Of2 m -> m.Name
        | Choice2Of2 p -> p.Name

    member fi.MethodInfo =
        match fi.Source with
        | Choice1Of2 m -> m
        | Choice2Of2 p -> p.GetGetMethod(true)

    member fi.IsProperty =
        match fi.Source with
        | Choice1Of2 _ -> false
        | Choice2Of2 _ -> true
    

/// Defines an untyped container for a cloud workflow
/// and related metadata.
[<AbstractClass>]
type CloudComputation internal () =

    /// Name given to the cloud computation.
    abstract Name : string
    /// Return type of the cloud computation.
    abstract ReturnType : Type
    /// Assemblies which computation depends on.
    abstract Dependencies : AssemblyId list
    /// Compiler warnings in cloud computation.
    abstract Warnings : string list
    /// Quoted version of the cloud workflow.
    abstract Expr : Expr option
    /// Function metadata for cloud computation.
    abstract Functions : FunctionInfo list
    /// Exitentially unpack the cloud contents.
    abstract Consume : ICloudComputationConsumer<'R> -> 'R

/// Abstract cloud computation unpacker
and ICloudComputationConsumer<'R> =
    abstract Consume<'T> : Cloud<'T> -> 'R

/// Abstract quotation evaluator library. Should be serializable.
type IQuotationEvaluator =
    abstract Eval : Quotations.Expr<'T> -> 'T

/// Defines a typed container for a cloud workflow
/// and related metadata.
[<AbstractClass>]
type CloudComputation<'T> internal () =
    inherit CloudComputation()

    /// Cloud workflow
    abstract Workflow : Cloud<'T>

    override __.ReturnType = typeof<'T>
    override __.Consume (r : ICloudComputationConsumer<'R>) = r.Consume __.Workflow

type internal BareCloudComputation<'T> (name : string, workflow : Cloud<'T>, warnings, dependencies) =
    inherit CloudComputation<'T> ()

    override __.Name = name
    override __.Dependencies = dependencies
    override __.Warnings = warnings
    override __.Workflow = workflow
    override __.Expr = None
    override __.Functions = []

type internal QuotedCloudComputation<'T>(name : string, expr : Expr<Cloud<'T>>, warnings, dependencies, functions, evaluator : IQuotationEvaluator) =
    inherit CloudComputation<'T> ()

    override __.Name = name
    override __.Dependencies = dependencies
    override __.Warnings = warnings
    override __.Workflow = evaluator.Eval expr
    override __.Expr = Some expr.Raw
    override __.Functions = functions

[<Serializable>]
type CompilerException = 
    inherit System.Exception

    override e.Message = 
        let name = 
            if String.IsNullOrEmpty e.Name then ""
            else
                sprintf "'%s' " e.Name

        e.Errors
        |> String.concat Environment.NewLine
        |> sprintf "Cloud workflow %sof type '%s' contains errors:%s%s" name Environment.NewLine (Type.prettyPrint e.Type)

    val public Name : string
    val public Type : Type
    val public Errors : string list
    val public Warnings : string list

    internal new (name : string, t : Type, errors : string list, warnings : string list) = 
        { 
            inherit System.Exception()
            Name = name
            Type = t
            Errors = errors
            Warnings = warnings
        }

    internal new (si : SerializationInfo, sc : StreamingContext) = 
        { 
            inherit System.Exception(si, sc)
            Name = si.GetString "name"
            Type = si.GetValue("type", typeof<Type>) :?> Type
            Errors = si.GetValue("errors", typeof<string list>) :?> string list
            Warnings = si.GetValue("warnings", typeof<string list>) :?> string list
        }

    interface ISerializable with
        member __.GetObjectData(si : SerializationInfo, sc : StreamingContext) =
            base.GetObjectData(si, sc)
            si.AddValue("name", __.Name)
            si.AddValue("type", __.Type)
            si.AddValue("errors", __.Errors)
            si.AddValue("warnings", __.Warnings)