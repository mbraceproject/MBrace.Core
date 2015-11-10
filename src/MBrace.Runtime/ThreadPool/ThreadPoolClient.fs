namespace MBrace.ThreadPool

open System.Threading

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.ThreadPool.Internals

/// Local file system CloudFilestore implementation
type FileSystemStore = MBrace.Runtime.Store.FileSystemStore
/// FsPickler Binary Serializer implementation
type FsPicklerBinarySerializer = MBrace.Runtime.FsPicklerBinarySerializer
/// FsPickler Xml Serializer implementation
type FsPicklerXmlSerializer = MBrace.Runtime.FsPicklerXmlSerializer
/// FsPickler Json Serializer implementation
type FsPicklerJsonSerializer = MBrace.Runtime.FsPicklerJsonSerializer
/// Json.Net Serializer implementation
type JsonDotNetSerializer = MBrace.Runtime.JsonDotNetSerializer

#nowarn "444"

/// Defines an MBrace thread pool runtime instance that is capable of
/// executing cloud workflows in the thread pool of the current process.
[<Sealed; AutoSerializable(false); NoEquality; NoComparison>]
type ThreadPoolRuntime private (resources : ResourceRegistry, _logger : ICloudLogger, _memoryEmulation : MemoryEmulation) =

    // Constructs a resource registry object with supplied paramaters
    let buildResources (memoryEmulation : MemoryEmulation) (logger : ICloudLogger option) (additionalResources : ResourceRegistry option) =
        let logger = defaultArg logger _logger
        let resources =
            match additionalResources with
            | None -> resources
            | Some resources' -> ResourceRegistry.Combine(resources, resources')

        let dp = ThreadPoolParallelismProvider.Create(logger, memoryEmulation)
        resources.Register<IParallelismProvider> dp

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
    static member CreateCancellationToken([<O;D(null:obj)>]?canceled:bool) =
        new ThreadPoolCancellationToken(new CancellationToken(canceled = defaultArg canceled false))
    
    /// <summary>
    ///     Creates a thread pool cancellation token wrapper to a System.Threading.CancellationToken
    /// </summary>
    /// <param name="sysToken">Input System.Threading.CancellationToken.</param>
    static member CreateCancellationToken(sysToken : CancellationToken) = new ThreadPoolCancellationToken(sysToken)

    /// Gets the default memory emulation mode used 
    /// in parallelism/store operations.
    member r.MemoryEmulation = _memoryEmulation

    /// Gets the ResourceRegistry used by the client instance
    member r.Resources = resources
    
    /// <summary>
    ///     Converts a cloud computation to an asynchronous workflow executed using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.ToAsync(workflow : Cloud<'T>, [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation, [<O;D(null:obj)>]?logger : ICloudLogger, [<O;D(null:obj)>]?resources : ResourceRegistry) : Async<'T> =
        let memoryEmulation = defaultArg memoryEmulation _memoryEmulation
        let resources = buildResources memoryEmulation logger resources
        Combinators.ToAsync(workflow, memoryEmulation, resources)

    /// <summary>
    ///     Executes a cloud computation using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.RunSynchronously(workflow : Cloud<'T>, [<O;D(null:obj)>]?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation, 
                                [<O;D(null:obj)>]?logger : ICloudLogger, [<O;D(null:obj)>]?resources : ResourceRegistry) : 'T =

        let memoryEmulation = defaultArg memoryEmulation _memoryEmulation
        let resources = buildResources memoryEmulation logger resources
        Combinators.RunSynchronously(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Executes a cloud computation using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.RunSynchronously(workflow : Cloud<'T>, cancellationToken : CancellationToken, [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation,
                                [<O;D(null:obj)>]?logger : ICloudLogger, [<O;D(null:obj)>]?resources : ResourceRegistry) : 'T =

        let ct = new ThreadPoolCancellationToken(cancellationToken)
        r.RunSynchronously(workflow, ct, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Executes a cloud computation as a local task using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.StartAsTask(workflow : Cloud<'T>, [<O;D(null:obj)>]?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation, 
                            [<O;D(null:obj)>]?logger : ICloudLogger, [<O;D(null:obj)>]?resources : ResourceRegistry) : ThreadPoolProcess<'T> =

        let memoryEmulation = defaultArg memoryEmulation _memoryEmulation
        let resources = buildResources memoryEmulation logger resources
        Combinators.StartAsTask(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Executes a cloud computation as a local task using parallelism
    ///     provided by the thread pool of the current process.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">CancellationToken used for workflow.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics during local parallel execution.</param>
    /// <param name="logger">Cloud logger implementation used in computation.</param>
    /// <param name="resources">Additional user-supplied resources for computation.</param>
    member r.StartAsTask(workflow : Cloud<'T>, cancellationToken : CancellationToken, [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation, 
                            [<O;D(null:obj)>]?logger : ICloudLogger, [<O;D(null:obj)>]?resources : ResourceRegistry) : ThreadPoolProcess<'T> =

        let ct = new ThreadPoolCancellationToken(cancellationToken)
        r.StartAsTask(workflow, ct, ?memoryEmulation = memoryEmulation, ?logger = logger, ?resources = resources)

    /// <summary>
    ///     Creates a ThreadPool runtime instance using provided resource components.
    /// </summary>
    /// <param name="logger">Logger abstraction. Defaults to no logging.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism. Defaults to shared memory.</param>
    /// <param name="fileStore">Cloud file store to be used. Defaults to random local FileSystemStore location.</param>
    /// <param name="serializer">Default serializer implementations. Defaults to FsPickler binary serializer.</param>
    /// <param name="textSerializer">Default text-based serializer implementations. Defaults to FsPickler json serializer.</param>
    /// <param name="valueProvider">CloudValue provider instance. Defaults to in-memory implementation.</param>
    /// <param name="atomProvider">Cloud atom provider instance. Defaults to in-memory atoms.</param>
    /// <param name="queueProvider">Cloud queue provider instance. Defaults to in-memory queues.</param>
    /// <param name="dictionaryProvider">Cloud dictionary configuration. Defaults to in-memory dictionary.</param>
    /// <param name="resources">Misc resources passed by user to execution context. Defaults to none.</param>
    static member Create([<O;D(null:obj)>]?logger : ICloudLogger,
                            [<O;D(null:obj)>]?memoryEmulation : MemoryEmulation,
                            [<O;D(null:obj)>]?fileStore : ICloudFileStore,
                            [<O;D(null:obj)>]?serializer : ISerializer,
                            [<O;D(null:obj)>]?textSerializer : ITextSerializer,
                            [<O;D(null:obj)>]?valueProvider : ICloudValueProvider,
                            [<O;D(null:obj)>]?atomProvider : ICloudAtomProvider,
                            [<O;D(null:obj)>]?queueProvider : ICloudQueueProvider,
                            [<O;D(null:obj)>]?dictionaryProvider : ICloudDictionaryProvider,
                            [<O;D(null:obj)>]?resources : ResourceRegistry) : ThreadPoolRuntime =

        let memoryEmulation = match memoryEmulation with Some m -> m | None -> MemoryEmulation.Shared
        let fileStore = match fileStore with Some fs -> fs | None -> FileSystemStore.CreateRandomLocal() :> _
        let serializer = match serializer with Some s -> s | None -> new FsPicklerBinarySerializer(useVagabond = false) :> _
        let textSerializer = 
            match textSerializer with
            | Some t -> t
            | None ->
                match serializer with
                | :? ITextSerializer as ts -> ts
                | _ -> new FsPicklerJsonSerializer(useVagabond = false) :> _

        let valueProvider = match valueProvider with Some vp -> vp | None -> new ThreadPoolValueProvider() :> _
        let atomProvider = match atomProvider with Some ap -> ap | None -> new ThreadPoolAtomProvider(memoryEmulation) :> _
        let dictionaryProvider = match dictionaryProvider with Some dp -> dp | None -> new ThreadPoolDictionaryProvider(memoryEmulation) :> _
        let queueProvider = match queueProvider with Some qp -> qp | None -> new ThreadPoolQueueProvider(memoryEmulation) :> _
        let logger = match logger with Some l -> l | None -> { new ICloudLogger with member __.Log _ = () }

        let resources = resource {
            yield serializer
            yield textSerializer
            yield fileStore
            yield valueProvider
            yield atomProvider
            yield dictionaryProvider
            yield queueProvider
            match resources with Some r -> yield! r | None -> ()
        }

        new ThreadPoolRuntime(resources, logger, memoryEmulation)