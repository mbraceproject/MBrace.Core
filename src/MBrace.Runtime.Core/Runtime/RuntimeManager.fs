namespace MBrace.Runtime

open System

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

/// Runtime provided runtime identifier
type IRuntimeId =
    inherit IComparable
    /// Runtime identifier
    abstract Id : string

/// Abstract MBrace runtime management object.
/// Contains all functionality necessary for the coordination of an MBrace cluster.
type IRuntimeManager =
    /// Runtime uniqueue identifier
    abstract Id : IRuntimeId
    /// FsPickler serialization instance used by the runtime
    abstract Serializer : FsPicklerSerializer
    /// Local MBrace resources to be supplied to workflow.
    abstract ResourceRegistry : ResourceRegistry
    /// Runtime worker manager object.
    abstract WorkerManager : IWorkerManager
    /// Abstraction used for managing logging by cloud workflows.
    abstract CloudLogManager : ICloudLogManager

    /// Logger abstraction used by the current process only.
    abstract SystemLogger : ISystemLogger

    /// Gets or sets the default log level for the local system logger.
    abstract LogLevel : LogLevel with get,set

    /// <summary>
    ///     Attaches a logger to the local process only.
    ///     Returns an uninstallation token.
    /// </summary>
    /// <param name="localLogger">Logger to be attached.</param>
    abstract AttachSystemLogger : localLogger:ISystemLogger -> IDisposable

    /// Gets the job queue instance for the runtime.
    abstract JobQueue : ICloudJobQueue
    /// Gets the Vagabond assembly manager for this runtime.
    abstract AssemblyManager : IAssemblyManager
    /// Gets the CloudTask manager instance for this runtime.
    abstract TaskManager : ICloudTaskManager
    /// Gets the distributed counter factory for this runtime.
    abstract CounterFactory : ICloudCounterFactory
    /// Gets the distributed result aggregator factory for this runtime,
    abstract ResultAggregatorFactory : ICloudResultAggregatorFactory
    /// Serializable distributed cancellation entry manager.
    abstract CancellationEntryFactory : ICancellationEntryFactory

    /// <summary>
    ///     Resets *all* cluster state.
    /// </summary>
    abstract ResetClusterState : unit -> Async<unit>