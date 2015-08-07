namespace MBrace.Runtime.InMemoryRuntime

open System.Threading

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

type Cloud =
    /// <summary>
    ///     Converts a cloud workflow to an async workflow with provided resources and 
    ///     cancellation token that is inherited from the async context.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="resources">ResourceRegistry for workflow. Defaults to the empty resource registry.</param>
    static member ToAsyncWithCurrentCancellationToken(workflow : Cloud<'T>, ?resources : ResourceRegistry) : Async<'T> = async {
        let resources = match resources with None -> ResourceRegistry.Empty | Some r -> r
        let! ct = Async.CancellationToken
        let cct = new InMemoryCancellationToken(ct)
        return! Cloud.ToAsync(workflow, resources, cct :> ICloudCancellationToken)
    }

/// Handle for in-memory execution of cloud workflows.
[<Sealed; AutoSerializable(false)>]
type InMemoryRuntime private (provider : ThreadPoolParallelismProvider, resources : ResourceRegistry, vagabondGraphChecker : (obj -> unit) option) =

    // set memory emulation semantics if specified at the method level
    let getResources (memoryEmulation : MemoryEmulation option) =
        match memoryEmulation with
        | Some me when provider.MemoryEmulation <> me ->
            let provider' = provider.WithMemoryEmulation me
            let resources = resources.Register<IParallelismProvider> provider'
            me, resources
        | _ -> provider.MemoryEmulation, resources

    /// ResourceRegistry used by the local runtime instance.
    member r.Resources = resources
    member r.MemoryEmulation = provider.MemoryEmulation
    
    /// <summary>
    ///     Asynchronously executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    member r.RunAsync(workflow : Cloud<'T>, ?memoryEmulation : MemoryEmulation) : Async<'T> =
        match vagabondGraphChecker with
        | Some v -> v workflow
        | None -> ()
        let mode, resources = getResources memoryEmulation
        ThreadPool.ToAsync(workflow, mode, resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    member r.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation) : 'T =
        match vagabondGraphChecker with
        | Some v -> v workflow
        | None -> ()
        let mode, resources = getResources memoryEmulation
        ThreadPool.RunSynchronously(workflow, mode, resources, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    member r.Run(workflow : Cloud<'T>, cancellationToken : CancellationToken, ?memoryEmulation : MemoryEmulation) : 'T =
        match vagabondGraphChecker with
        | Some v -> v workflow
        | None -> ()
        let cancellationToken = new InMemoryCancellationToken(cancellationToken)
        let mode, resources = getResources memoryEmulation
        ThreadPool.RunSynchronously(workflow, mode, resources, cancellationToken)

    /// Creates a new cancellation token source
    member r.CreateCancellationTokenSource() = 
        InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource [||] :> ICloudCancellationTokenSource

    /// <summary>
    ///     Creates an InMemory runtime instance with provided store components.
    /// </summary>
    /// <param name="logger">Logger abstraction. Defaults to no logging.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism. Defaults to shared memory.</param>
    /// <param name="fileConfig">File store configuration. Defaults to no file store.</param>
    /// <param name="serializer">Default serializer implementations. Defaults to no serializer.</param>
    /// <param name="valueProvider">CloudValue provider instance. Defaults to in-memory implementation.</param>
    /// <param name="atomConfig">Cloud atom configuration. Defaults to in-memory atoms.</param>
    /// <param name="queueConfig">Cloud queue configuration. Defaults to in-memory queues.</param>
    /// <param name="dictionaryProvider">Cloud dictionary configuration. Defaults to in-memory dictionary.</param>
    /// <param name="resources">Misc resources passed by user to execution context. Defaults to none.</param>
    /// <param name="vagabondChecker">User-supplied workflow checker function.</param>
    static member Create(?logger : ICloudLogger,
                            ?memoryEmulation : MemoryEmulation,
                            ?fileConfig : CloudFileStoreConfiguration,
                            ?serializer : ISerializer,
                            ?valueProvider : ICloudValueProvider,
                            ?atomConfig : CloudAtomConfiguration,
                            ?queueConfig : CloudQueueConfiguration,
                            ?dictionaryProvider : ICloudDictionaryProvider,
                            ?resources : ResourceRegistry,
                            ?vagabondChecker : obj -> unit) : InMemoryRuntime =

        let memoryEmulation = match memoryEmulation with Some m -> m | None -> MemoryEmulation.Shared
        let parallelismProvider = ThreadPoolParallelismProvider.Create(?logger = logger, memoryEmulation = memoryEmulation)
        let valueProvider = match valueProvider with Some vp -> vp | None -> new InMemoryValueProvider() :> _
        let atomConfig = match atomConfig with Some ac -> ac | None -> InMemoryAtomProvider.CreateConfiguration(memoryEmulation)
        let dictionaryProvider = match dictionaryProvider with Some dp -> dp | None -> new InMemoryDictionaryProvider(memoryEmulation) :> _
        let queueConfig = match queueConfig with Some cc -> cc | None -> InMemoryQueueProvider.CreateConfiguration(memoryEmulation)

        let resources = resource {
            yield valueProvider
            yield atomConfig
            yield dictionaryProvider
            yield queueConfig
            match fileConfig with Some fc -> yield fc | None -> ()
            match serializer with Some sr -> yield sr | None -> ()
            match resources with Some r -> yield! r | None -> ()
            yield parallelismProvider :> IParallelismProvider
        }

        new InMemoryRuntime(parallelismProvider, resources, vagabondChecker)