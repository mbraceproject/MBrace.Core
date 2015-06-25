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
    /// Logger abstraction for user-specified cloud logging.
    abstract GetCloudLogger : worker:IWorkerId * job:CloudJob -> ICloudLogger
    /// Logger abstraction used for local worker logging.
    abstract SystemLogger : ISystemLogger
    /// Gets the job queue instance for the runtime.
    abstract JobQueue : IJobQueue
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