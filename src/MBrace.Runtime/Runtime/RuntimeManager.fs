namespace MBrace.Runtime

open System
open System.Collections.Concurrent

open MBrace.FsPickler
open MBrace.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils

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

    /// Abstraction used for managing system logs in the local process.
    abstract LocalSystemLogManager : ILocalSystemLogManager
    /// Abstraction used for managing runtime-wide system logs.
    abstract RuntimeSystemLogManager : IRuntimeSystemLogManager
    /// Abstraction used for managing logging by cloud workflows.
    abstract CloudLogManager : ICloudLogManager

    /// Runtime worker manager object.
    abstract WorkerManager : IWorkerManager
    /// Gets the work item queue instance for the runtime.
    abstract WorkItemQueue : ICloudWorkItemQueue
    /// Gets the Vagabond assembly manager for this runtime.
    abstract AssemblyManager : IAssemblyManager
    /// Gets the CloudProcess manager instance for this runtime.
    abstract ProcessManager : ICloudProcessManager
    /// Gets the distributed counter factory for this runtime.
    abstract CounterFactory : ICloudCounterFactory
    /// Gets the distributed result aggregator factory for this runtime,
    abstract ResultAggregatorFactory : ICloudResultAggregatorFactory
    /// Serializable distributed cancellation entry manager.
    abstract CancellationEntryFactory : ICancellationEntryFactory

    /// Resets the entire cluster state
    abstract ResetClusterState : unit -> Async<unit>

[<AutoOpen>]
module RuntimeManagerExtensions =
    
    type IRuntimeManager with
        member inline r.SystemLogger = r.LocalSystemLogManager.Logger

/// Global Registry for loading IRuntimeManager instances on primitive deserialization
type RuntimeManagerRegistry private () =
    static let container = new ConcurrentDictionary<Type * IRuntimeId, IRuntimeManager> ()

    static let getKey(runtimeId : IRuntimeId) = runtimeId.GetType(), runtimeId

    /// <summary>
    ///     Attempt to register a runtime manager instance.
    /// </summary>
    /// <param name="runtime">Runtime manager instance to be registered.</param>
    static member TryRegister(runtime : IRuntimeManager) : bool =
        if obj.ReferenceEquals(runtime, null) then raise <| new ArgumentNullException("runtime")
        container.TryAdd(getKey runtime.Id, runtime)

    /// <summary>
    ///     Resolve locally registered runtime manager instance by runtime id.
    /// </summary>
    /// <param name="id">Runtime identifier.</param>
    static member Resolve(id : IRuntimeId) : IRuntimeManager =
        let mutable runtime = Unchecked.defaultof<_>
        if container.TryGetValue(getKey id, &runtime) then runtime
        else
            invalidOp <| sprintf "RuntimeManagerRegistry: could not locate registered instance for runtime '%O'." id

    /// <summary>
    ///     Attempt to unregister a runtime manager instance by id.
    /// </summary>
    /// <param name="id">Runtime identifier.</param>
    static member TryRemove(id : IRuntimeId) : bool = 
        let ok,_ = container.TryRemove (getKey id) in ok