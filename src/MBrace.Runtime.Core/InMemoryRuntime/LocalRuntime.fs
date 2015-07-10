namespace MBrace.Runtime.InMemoryRuntime

open System.Threading

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Handle for in-memory execution of cloud workflows.
[<Sealed; AutoSerializable(false)>]
type LocalRuntime private (resources : ResourceRegistry) =

    let storeClient = CloudStoreClient.CreateFromResources(resources)

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
    ///     Creates an InMemory runtime instance with provided store components.
    /// </summary>
    /// <param name="logger">Logger abstraction. Defaults to no logging.</param>
    /// <param name="memoryMode">Memory semantics used for parallelism. Defaults to shared memory.</param>
    /// <param name="fileConfig">File store configuration. Defaults to no file store.</param>
    /// <param name="serializer">Default serializer implementations. Defaults to no serializer.</param>
    /// <param name="valueProvider">CloudValue provider instance. Defaults to in-memory implementation.</param>
    /// <param name="atomConfig">Cloud atom configuration. Defaults to in-memory atoms.</param>
    /// <param name="queueConfig">Cloud queue configuration. Defaults to in-memory queues.</param>
    /// <param name="resources">Misc resources passed by user to execution context. Defaults to none.</param>
    static member Create(?logger : ICloudLogger,
                            ?memoryMode : MemoryEmulation,
                            ?fileConfig : CloudFileStoreConfiguration,
                            ?serializer : ISerializer,
                            ?valueProvider : ICloudValueProvider,
                            ?atomConfig : CloudAtomConfiguration,
                            ?queueConfig : CloudQueueConfiguration,
                            ?dictionaryProvider : ICloudDictionaryProvider,
                            ?resources : ResourceRegistry) : LocalRuntime =

        let memoryMode = match memoryMode with Some m -> m | None -> MemoryEmulation.Shared
        let valueProvider = match valueProvider with Some vp -> vp | None -> new InMemoryValueProvider() :> _
        let atomConfig = match atomConfig with Some ac -> ac | None -> InMemoryAtomProvider.CreateConfiguration()
        let dictionaryProvider = match dictionaryProvider with Some dp -> dp | None -> new InMemoryDictionaryProvider() :> _
        let queueConfig = match queueConfig with Some cc -> cc | None -> InMemoryQueueProvider.CreateConfiguration()

        let resources = resource {
            yield ThreadPoolRuntime.Create(?logger = logger, ?memoryMode = memoryMode) :> IDistributionProvider
            yield valueProvider
            yield atomConfig
            yield dictionaryProvider
            yield queueConfig
            match fileConfig with Some fc -> yield fc | None -> ()
            match serializer with Some sr -> yield sr | None -> ()
            match resources with Some r -> yield! r | None -> ()
        }

        new LocalRuntime(resources)