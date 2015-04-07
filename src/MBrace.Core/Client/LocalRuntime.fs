namespace MBrace.Client

open System.Threading

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.InMemory

#nowarn "444"

/// Cloud cancellation token implementation that wraps around System.Threading.CancellationToken
type InMemoryCancellationToken = MBrace.Runtime.InMemory.InMemoryCancellationToken
/// Cloud cancellation token source implementation that wraps around System.Threading.CancellationTokenSource
type InMemoryCancellationTokenSource = MBrace.Runtime.InMemory.InMemoryCancellationTokenSource
/// Cloud task implementation that wraps around System.Threading.Task for inmemory runtimes
type InMemoryTask<'T> = MBrace.Runtime.InMemory.InMemoryTask<'T>

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
        toLocalAsync resources workflow

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    member r.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken) : 'T =
        let cancellationToken = match cancellationToken with Some ct -> ct | None -> new InMemoryCancellationToken() :> _
        Cloud.RunSynchronously(workflow, resources, cancellationToken)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    member r.Run(workflow : Cloud<'T>, cancellationToken : CancellationToken) : 'T =
        let cancellationToken = new InMemoryCancellationToken(cancellationToken)
        Cloud.RunSynchronously(workflow, resources, cancellationToken)

    /// Creates a new cancellation token source
    member r.CreateCancellationTokenSource() = 
        InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource [||] :> ICloudCancellationTokenSource

    /// <summary>
    ///     Creates an InMemory runtime instance with provided resources.
    /// </summary>
    /// <param name="resources"></param>
    static member Create(resources : ResourceRegistry) =
        new LocalRuntime(resources)

    /// <summary>
    ///     Creates an InMemory runtime instance with provided store components.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="logger">Logger abstraction. Defaults to no logging.</param>
    /// <param name="fileConfig">File store configuration. Defaults to no file store.</param>
    /// <param name="serializer">Default serializer implementations. Defaults to no serializer.</param>
    /// <param name="objectCache">Object caching instance. Defaults to no cache.</param>
    /// <param name="atomConfig">Cloud atom configuration. Defaults to in-memory atoms.</param>
    /// <param name="channelConfig">Cloud channel configuration. Defaults to in-memory channels.</param>
    /// <param name="resources">Misc resources passed by user to execution context. Defaults to none.</param>
    static member Create(?logger : ICloudLogger,
                            ?fileConfig : CloudFileStoreConfiguration,
                            ?serializer : ISerializer,
                            ?objectCache : IObjectCache,
                            ?atomConfig : CloudAtomConfiguration,
                            ?channelConfig : CloudChannelConfiguration,
                            ?dictionaryProvider : ICloudDictionaryProvider,
                            ?resources : ResourceRegistry) : LocalRuntime =

        let atomConfig = match atomConfig with Some ac -> ac | None -> InMemoryAtomProvider.CreateConfiguration()
        let dictionaryProvider = match dictionaryProvider with Some dp -> dp | None -> new InMemoryDictionaryProvider() :> _
        let channelConfig = match channelConfig with Some cc -> cc | None -> InMemoryChannelProvider.CreateConfiguration()

        let resources = resource {
            yield ThreadPoolRuntime.Create(?logger = logger) :> IDistributionProvider
            yield atomConfig
            yield dictionaryProvider
            yield channelConfig
            match fileConfig with Some fc -> yield fc | None -> ()
            match serializer with Some sr -> yield sr | None -> ()
            match objectCache with Some oc -> yield oc | None -> ()
            match resources with Some r -> yield! r | None -> ()
        }

        new LocalRuntime(resources)