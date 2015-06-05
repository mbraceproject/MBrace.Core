namespace MBrace.Runtime

open System

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

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

/// Defines an abstract runtime resource provider. Should not be serializable
type IRuntimeResourceManager =
    /// Local MBrace resources to be supplied to workflow.
    abstract ResourceRegistry : ResourceRegistry
    /// Asynchronously returns all workers in the current cluster.
    abstract GetAvailableWorkers : unit -> Async<IWorkerRef []>
    /// Gets the WorkerRef identifying the current worker instance.
    abstract CurrentWorker : IWorkerRef
    /// Logger abstraction for user-specified cloud logging.
    abstract GetCloudLogger : CloudJob -> ICloudLogger
    /// Logger abstraction used for local worker logging.
    abstract SystemLogger : ISystemLogger
    /// Serializable distributed cancellation entry manager.
    abstract CancellationEntryFactory : ICancellationEntryFactory
    /// Gets the job queue instance for the runtime.
    abstract JobQueue : IJobQueue
    /// Gets the Vagabond assembly manager for this runtime.
    abstract AssemblyManager : IAssemblyManager

    /// <summary>
    ///     Asynchronously creates a new distributed counter with supplied initial value.
    /// </summary>
    /// <param name="initialValue">Supplied initial value.</param>
    abstract RequestCounter : initialValue:int -> Async<ICloudCounter>

    /// <summary>
    ///     Asynchronously creates a new result aggregator with supplied capacity.
    /// </summary>
    /// <param name="capacity">Number of items to be aggregated.</param>
    abstract RequestResultAggregator<'T> : capacity:int -> Async<IResultAggregator<'T>>

    /// Asynchronously creates a new distributed task completion source.
    abstract RequestTaskCompletionSource<'T> : unit -> Async<ICloudTaskCompletionSource<'T>>