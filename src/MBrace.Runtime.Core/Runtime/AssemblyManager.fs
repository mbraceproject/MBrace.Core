namespace MBrace.Runtime

open Nessos.Vagabond

/// Defines a Vagabond assembly manager
type IAssemblyManager =
    /// <summary>
    ///     Uploads assemblies and data dependencies to runtime.
    /// </summary>
    /// <param name="assemblies">Local Vagabond assembly descriptors to be uploaded.</param>
    abstract UploadAssemblies : assemblies:seq<VagabondAssembly> -> Async<unit>

    /// <summary>
    ///     Downloads assemblies and data dependencies from runtime.
    /// </summary>
    /// <param name="ids">Assembly identifiers to be downloaded.</param>
    abstract DownloadAssemblies : ids:seq<AssemblyId> -> Async<VagabondAssembly []>

    /// <summary>
    ///     Loads provided vagabond assemblies to current application domain.
    /// </summary>
    /// <param name="assemblies">Assemblies to be loaded.</param>
    abstract LoadAssemblies : assemblies:seq<VagabondAssembly> -> AssemblyLoadInfo []

    /// <summary>
    ///     Computes vagabond dependencies for provided object graph.
    /// </summary>
    /// <param name="graph">Object graph to be computed.</param>
    abstract ComputeDependencies : graph:obj -> VagabondAssembly []

    /// <summary>
    ///     Registers a native dependency for instance.
    /// </summary>
    /// <param name="path"></param>
    abstract RegisterNativeDependency : path:string -> VagabondAssembly

    /// Gets native dependencies for assembly
    abstract NativeDependencies : VagabondAssembly []