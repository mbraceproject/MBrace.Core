namespace MBrace.Client

open System.Threading

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.InMemory

/// Handle for in-memory execution of cloud workflows.
[<Sealed; AutoSerializable(false)>]
type LocalRuntime private (resources : ResourceRegistry) =

    let storeClient = StoreClient.CreateFromResources(resources)

    /// Store client instance for in memory runtime instance.
    member r.StoreClient = storeClient
    
    /// <summary>
    ///     Asynchronously executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    member r.RunAsync(workflow : Cloud<'T>) : Async<'T> =
        Cloud.ToAsync(workflow, resources = resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    member r.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) : 'T =
        Cloud.RunSynchronously(workflow, resources = resources, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Creates an InMemory runtime instance with provided store components.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="logger">Logger abstraction. Defaults to no logging.</param>
    /// <param name="fileConfig">File store configuration. Defaults to no file store.</param>
    /// <param name="atomConfig">Cloud atom configuration. Defaults to in-memory atoms.</param>
    /// <param name="channelConfig">Cloud channel configuration. Defaults to in-memory channels.</param>
    /// <param name="resources">Misc resources passed by user to execution context. Defaults to none.</param>
    static member Create(?logger : ICloudLogger,
                            ?fileConfig : CloudFileStoreConfiguration,
                            ?atomConfig : CloudAtomConfiguration,
                            ?channelConfig : CloudChannelConfiguration,
                            ?resources : ResourceRegistry) : LocalRuntime =

        let atomConfig = match atomConfig with Some ac -> ac | None -> InMemoryAtomProvider.CreateConfiguration()
        let channelConfig = match channelConfig with Some cc -> cc | None -> InMemoryChannelProvider.CreateConfiguration()

        let resources = resource {
            yield ThreadPoolRuntime.Create(?logger = logger) :> ICloudRuntimeProvider
            yield atomConfig
            yield channelConfig
            match fileConfig with Some fc -> yield fc | None -> ()
            match resources with Some r -> yield! r | None -> ()
        }

        new LocalRuntime(resources)