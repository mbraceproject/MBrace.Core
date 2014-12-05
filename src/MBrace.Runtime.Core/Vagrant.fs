namespace Nessos.MBrace.Runtime.Vagrant

open System
open System.Reflection
open System.IO

open Nessos.FsPickler
open Nessos.Vagrant

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime.Utils
open Nessos.MBrace.Runtime.Utils.Retry
open Nessos.MBrace.Runtime.Serialization

/// Vagrant state container
type VagrantRegistry private () =

    static let vagrantInstance : Vagrant option ref = ref None
    static let serializer : ISerializer option ref = ref None

    static let ignoredAssemblies = 
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        hset dependencies

    /// Gets the registered vagrant instance.
    static member Vagrant =
        match vagrantInstance.Value with
        | None -> invalidOp "No instance of vagrant has been registered."
        | Some instance -> instance

    /// Gets the registered FsPickler serializer instance.
    static member Pickler = VagrantRegistry.Vagrant.Pickler

    static member Serializer =
        match serializer.Value with
        | None -> invalidOp "No instance of vagrant has been registered."
        | Some s -> s

    /// <summary>
    ///     Computes assembly dependencies for given serializable object graph.
    /// </summary>
    /// <param name="graph">Object graph.</param>
    static member ComputeObjectDependencies(graph : obj) =
        VagrantRegistry.Vagrant.ComputeObjectDependencies(graph, permitCompilation = true)
        |> List.map Utilities.ComputeAssemblyId

    /// <summary>
    ///     Initializes the registry using provided factory.
    /// </summary>
    /// <param name="factory">Vagrant instance factory.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize(factory : unit -> Vagrant, ?throwOnError) =
        lock vagrantInstance (fun () ->
            match vagrantInstance.Value with
            | None -> 
                let v = factory ()
                let s = FsPicklerStoreSerializer.CreateAndRegister(v.Pickler, "Vagrant pickler")
                vagrantInstance := Some v
                serializer := Some (s :> ISerializer)

            | Some _ when defaultArg throwOnError true -> invalidOp "An instance of Vagrant has already been registered."
            | Some _ -> ())

    /// <summary>
    ///     Initializes vagrant using default settings.
    /// </summary>
    /// <param name="ignoreAssembly">Specify an optional ignore assembly predicate.</param>
    /// <param name="loadPolicy">Specify a default assembly load policy.</param>
    /// <param name="throwOnError">Throw exception on error.</param>
    static member Initialize (?ignoreAssembly : Assembly -> bool, ?loadPolicy, ?throwOnError) =
        let ignoreAssembly (a : Assembly) = ignoredAssemblies.Contains a || ignoreAssembly |> Option.exists (fun i -> i a)
        VagrantRegistry.Initialize((fun () ->
            let cachePath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-%O" <| Guid.NewGuid())
            let dir = retry (RetryPolicy.Retry(3, delay = 0.2<sec>)) (fun () -> Directory.CreateDirectory cachePath)
            Vagrant.Initialize(cacheDirectory = cachePath, isIgnoredAssembly = ignoreAssembly, ?loadPolicy = loadPolicy)), ?throwOnError = throwOnError)