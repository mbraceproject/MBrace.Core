namespace MBrace.Runtime.Vagabond

open System
open System.Reflection
open System.IO

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Store
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry
open MBrace.Runtime.Serialization

/// Vagabond state container
type VagabondRegistry private () =

    static let vagabondInstance : Vagabond option ref = ref None
    static let serializer : ISerializer option ref = ref None

    /// Gets the registered vagabond instance.
    static member Vagabond =
        match vagabondInstance.Value with
        | None -> invalidOp "No instance of vagabond has been registered."
        | Some instance -> instance

    /// Gets the registered FsPickler serializer instance.
    static member Pickler = VagabondRegistry.Vagabond.Pickler

    static member Serializer =
        match serializer.Value with
        | None -> invalidOp "No instance of vagabond has been registered."
        | Some s -> s

    /// <summary>
    ///     Computes assembly dependencies for given serializable object graph.
    /// </summary>
    /// <param name="graph">Object graph.</param>
    static member ComputeObjectDependencies(graph : obj) =
        VagabondRegistry.Vagabond.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId

    /// <summary>
    ///     Initializes the registry using provided factory.
    /// </summary>
    /// <param name="factory">Vagabond instance factory.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize(factory : unit -> Vagabond, ?throwOnError) =
        lock vagabondInstance (fun () ->
            match vagabondInstance.Value with
            | None -> 
                let v = factory ()
                vagabondInstance := Some v
                serializer := Some (
                    { new FsPicklerStoreSerializer () with
                        member __.Id = "VagabondSerializer"
                        member __.Serializer = VagabondRegistry.Pickler :> _
                    } :> ISerializer)

            | Some _ when defaultArg throwOnError true -> invalidOp "An instance of Vagabond has already been registered."
            | Some _ -> ())

    /// <summary>
    ///     Initializes vagabond using default settings.
    /// </summary>
    /// <param name="ignoreAssembly">Specify an optional ignore assembly predicate.</param>
    /// <param name="loadPolicy">Specify a default assembly load policy.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize (?ignoredAssemblies : seq<Assembly>, ?loadPolicy, ?throwOnError) =
        let ignoredAssemblies = seq { 
            yield Assembly.GetExecutingAssembly() 
            match ignoredAssemblies with 
            | None -> () 
            | Some ias -> yield! ias
        }

        VagabondRegistry.Initialize((fun () ->
            let cachePath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-%O" <| Guid.NewGuid())
            let dir = retry (RetryPolicy.Retry(3, delay = 0.2<sec>)) (fun () -> Directory.CreateDirectory cachePath)
            Vagabond.Initialize(cacheDirectory = cachePath, ignoredAssemblies = ignoredAssemblies, ?loadPolicy = loadPolicy)), ?throwOnError = throwOnError)