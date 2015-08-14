namespace MBrace.ThreadPool

open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.ThreadPool.Internals

#nowarn "444"

/// Defines a client for sending cloud computation for execution using
/// the thread pool of the current process.
[<Sealed; AutoSerializable(false)>]
type ThreadPoolClient private (resources : ResourceRegistry, defaultLogger : ICloudLogger, defaultMemoryEmulation : MemoryEmulation, vagabondGraphChecker : (obj -> unit) option) =

    let mutable defaultMemoryEmulation = defaultMemoryEmulation

    let getResources (memoryEmulation : MemoryEmulation) (logger : ICloudLogger option) (additionalResources : ResourceRegistry option) =
        let logger = defaultArg logger defaultLogger
        let resources =
            match additionalResources with
            | None -> resources
            | Some resources' -> ResourceRegistry.Combine(resources, resources')

        let dp = ThreadPoolParallelismProvider.Create(logger, memoryEmulation)
        resources.Register<IParallelismProvider> dp

    /// Gets or sets the default closure serialization
    /// emulation mode use in parallel workflow execution.
    member r.MemoryEmulation
        with get () = defaultMemoryEmulation
        and set me = defaultMemoryEmulation <- me

    /// Gets the ResourceRegistry used by the client instance
    member r.Resources = resources
    
    /// <summary>
    ///     Converts a cloud workflow to an asynchronous workflow executed using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.ToAsync(workflow : Cloud<'T>, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : Async<'T> =
        match vagabondGraphChecker with Some v -> v workflow | None -> ()
        let memoryEmulation = defaultArg memoryEmulation defaultMemoryEmulation
        let resources = getResources memoryEmulation logger resources
        Combinators.ToAsync(workflow, memoryEmulation, resources)

    /// <summary>
    ///     Executes a cloud workflow using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.RunSynchronously(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : 'T =
        match vagabondGraphChecker with Some v -> v workflow | None -> ()
        let memoryEmulation = defaultArg memoryEmulation defaultMemoryEmulation
        let resources = getResources memoryEmulation logger resources
        Combinators.RunSynchronously(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Executes a cloud workflow using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.RunSynchronously(workflow : Cloud<'T>, cancellationToken : CancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : 'T =
        let ct = new ThreadPoolCancellationToken(cancellationToken)
        r.RunSynchronously(workflow, ct, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud workflow as a local task using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.StartAsTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : ICloudTask<'T> =
        match vagabondGraphChecker with Some v -> v workflow | None -> ()
        let memoryEmulation = defaultArg memoryEmulation defaultMemoryEmulation
        let resources = getResources memoryEmulation logger resources
        Combinators.StartAsTask(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken) :> ICloudTask<'T>

    /// <summary>
    ///     Executes a cloud workflow as a local task using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.StartAsTask(workflow : Cloud<'T>, cancellationToken : CancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : ICloudTask<'T> =
        let ct = new ThreadPoolCancellationToken(cancellationToken)
        r.StartAsTask(workflow, ct, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Creates a ThreadPool client instance using provided resource components.
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
    /// <param name="vagabondChecker">User-supplied workflow dependency checker function.</param>
    static member Create(?logger : ICloudLogger,
                            ?memoryEmulation : MemoryEmulation,
                            ?fileConfig : CloudFileStoreConfiguration,
                            ?serializer : ISerializer,
                            ?valueProvider : ICloudValueProvider,
                            ?atomConfig : CloudAtomConfiguration,
                            ?queueConfig : CloudQueueConfiguration,
                            ?dictionaryProvider : ICloudDictionaryProvider,
                            ?resources : ResourceRegistry,
                            ?vagabondChecker : obj -> unit) : ThreadPoolClient =

        let memoryEmulation = match memoryEmulation with Some m -> m | None -> MemoryEmulation.Shared
        let valueProvider = match valueProvider with Some vp -> vp | None -> new ThreadPoolValueProvider() :> _
        let atomConfig = match atomConfig with Some ac -> ac | None -> ThreadPoolAtomProvider.CreateConfiguration(memoryEmulation)
        let dictionaryProvider = match dictionaryProvider with Some dp -> dp | None -> new ThreadPoolDictionaryProvider(memoryEmulation) :> _
        let queueConfig = match queueConfig with Some cc -> cc | None -> ThreadPoolQueueProvider.CreateConfiguration(memoryEmulation)
        let logger = match logger with Some l -> l | None -> { new ICloudLogger with member __.Log _ = () }

        let resources = resource {
            yield valueProvider
            yield atomConfig
            yield dictionaryProvider
            yield queueConfig
            match fileConfig with Some fc -> yield fc | None -> ()
            match serializer with Some sr -> yield sr | None -> ()
            match resources with Some r -> yield! r | None -> ()
        }

        new ThreadPoolClient(resources, logger, memoryEmulation, vagabondChecker)


/// MBrace thread pool runtime methods
type ThreadPool private () =

    static let singleton = lazy(ThreadPoolClient.Create())

    /// Creates a new thread pool cancellation token source instance.
    static member CreateCancellationTokenSource() = new ThreadPoolCancellationTokenSource() :> ICloudCancellationTokenSource

    /// <summary>
    ///     Creates a thread pool cancellation token source linked to a collection of parent tokens.
    /// </summary>
    /// <param name="parents">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(parents : seq<#ICloudCancellationToken>) =
        ThreadPoolCancellationTokenSource.CreateLinkedCancellationTokenSource(parents |> Seq.map unbox |> Seq.toArray)

    /// <summary>
    ///     Creates a fresh thread pool cancellation token.
    /// </summary>
    /// <param name="canceled">Create as canceled token. Defaults to false.</param>
    static member CreateCancellationToken(?canceled:bool) =
        new ThreadPoolCancellationToken(new CancellationToken(canceled = defaultArg canceled false))
    
    /// <summary>
    ///     Creates a thread pool cancellation token wrapper to a System.Threading.CancellationToken
    /// </summary>
    /// <param name="sysToken">Input System.Threading.CancellationToken.</param>
    static member CreateCancellationToken(sysToken : CancellationToken) = new ThreadPoolCancellationToken(sysToken)

    /// Gets or sets the default closure serialization
    /// emulation mode use in parallel workflow execution.
    static member MemoryEmulation
        with get () = singleton.Value.MemoryEmulation
        and set me = singleton.Value.MemoryEmulation <- me

    /// <summary>
    ///     Asynchronously executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    static member ToAsync(workflow : Cloud<'T>, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : Async<'T> =
        singleton.Value.ToAsync(workflow, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    static member RunSynchronously(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : 'T =
       singleton.Value.RunSynchronously(workflow, ?cancellationToken = cancellationToken, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token passed to computation.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    static member RunSynchronously(workflow : Cloud<'T>, cancellationToken : CancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : 'T =
        singleton.Value.RunSynchronously(workflow, cancellationToken, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="memoryEmulation"></param>
    /// <param name="logger"></param>
    /// <param name="resources"></param>
    static member StartAsTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : ICloudTask<'T> =
        singleton.Value.StartAsTask(workflow, ?cancellationToken = cancellationToken, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud computation in the local process,
    ///     with parallelism provided by the .NET thread pool.
    /// </summary>
    /// <param name="workflow"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="memoryEmulation"></param>
    /// <param name="logger"></param>
    /// <param name="resources"></param>
    static member StartAsTask(workflow : Cloud<'T>, cancellationToken : CancellationToken, ?memoryEmulation : MemoryEmulation, ?logger : ICloudLogger, ?resources : ResourceRegistry) : ICloudTask<'T> =
        singleton.Value.StartAsTask(workflow, cancellationToken = cancellationToken, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)