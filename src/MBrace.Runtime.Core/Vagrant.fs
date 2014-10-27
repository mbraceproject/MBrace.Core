namespace Nessos.MBrace.Runtime

open System
open System.Reflection
open System.IO

open Nessos.Vagrant

open Nessos.MBrace.Runtime.Utils.Retry

/// <summary>
///     Contains the default instance of a Vagrant instance.
/// </summary>
type VagrantRegistry private () =

    static let instance : Vagrant option ref = ref None

    static let ignoredAssemblies = 
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        new System.Collections.Generic.HashSet<_>(dependencies)

    /// Gets the registered vagrant instance.
    static member Vagrant =
        match instance.Value with
        | None -> invalidOp "No instance of vagrant has been registered."
        | Some instance -> instance

    /// Gets the registered FsPickler serializer instance.
    static member Pickler = VagrantRegistry.Vagrant.Pickler

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
    static member Initialize(factory : unit -> Vagrant) =
        lock instance (fun () ->
            match instance.Value with
            | None -> instance := Some (factory ())
            | Some _ -> invalidOp "An instance of Vagrant has already been registered.")

    /// <summary>
    ///     Initializes vagrant using default settings.
    /// </summary>
    static member Initialize () =
        VagrantRegistry.Initialize(fun () ->
            let cachePath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-%O" <| Guid.NewGuid())
            let dir = retry (RetryPolicy.Retry(3, delay = 0.2<sec>)) (fun () -> Directory.CreateDirectory cachePath)
            Vagrant.Initialize(cacheDirectory = cachePath, isIgnoredAssembly = ignoredAssemblies.Contains)
        )