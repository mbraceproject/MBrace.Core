namespace MBrace.Runtime

open System

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals

/// Abstract MBrace runtime management object.
/// Contains all functionality necessary for the coordination of an MBrace cluster.
type IRuntimeManager =
    /// Runtime uniqueue identifier
    abstract Id : string
    /// Local MBrace resources to be supplied to workflow.
    abstract ResourceRegistry : ResourceRegistry
    /// Runtime worker manager object.
    abstract WorkerManager : IWorkerManager
    /// Logger abstraction for user-specified cloud logging.
    abstract GetCloudLogger : CloudJob -> ICloudLogger
    /// Logger abstraction used for local worker logging.
    abstract SystemLogger : ISystemLogger
    /// Gets the job queue instance for the runtime.
    abstract JobQueue : IJobQueue
    /// Gets the Vagabond assembly manager for this runtime.
    abstract AssemblyManager : IAssemblyManager
    /// Gets the CloudTask manager instance for this runtime.
    abstract TaskManager : ICloudTaskManager
    /// Gets the distributed primitives factory for this runtime
    abstract PrimitivesFactory : ICloudPrimitivesFactory
    /// Serializable distributed cancellation entry manager.
    abstract CancellationEntryFactory : ICancellationEntryFactory

    /// <summary>
    ///     Resets *all* cluster state.
    /// </summary>
    abstract ResetClusterState : unit -> Async<unit>