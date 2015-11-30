namespace MBrace.Runtime

open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals
open MBrace.ThreadPool
open MBrace.Runtime.Utils

/// MBrace runtime client handle abstract class.
[<AbstractClass; NoEquality; NoComparison; AutoSerializable(false)>]
type MBraceClient (runtime : IRuntimeManager, defaultFaultPolicy : FaultPolicy) =
    do ignore <| RuntimeManagerRegistry.TryRegister runtime

    let syncRoot = new obj()
    let imem = ThreadPoolRuntime.Create(resources = runtime.ResourceRegistry, memoryEmulation = MemoryEmulation.Shared)
    let storeClient = new CloudStoreClient(runtime.ResourceRegistry)
    let mutable defaultFaultPolicy = defaultFaultPolicy

    let taskManagerClient = new CloudProcessManagerClient(runtime)
    let getWorkers () = async {
        let! workers = runtime.WorkerManager.GetAvailableWorkers()
        return workers |> Array.map (fun w -> WorkerRef.Create(runtime, w.Id))
    }

    let workers = CacheAtom.Create(getWorkers(), intervalMilliseconds = 1000)

    let mutable systemLogPoller : ILogPoller<SystemLogEntry> option = None
    let getSystemLogPoller() =
        match systemLogPoller with
        | Some lp -> lp
        | None ->
            lock syncRoot (fun () ->
                match systemLogPoller with
                | Some lp -> lp
                | None ->
                    let lp = runtime.RuntimeSystemLogManager.CreateLogPoller() |> Async.RunSync
                    systemLogPoller <- Some lp
                    lp)

    /// <summary>
    ///     Creates a fresh cloud cancellation token source for use in the MBrace cluster.
    /// </summary>
    /// <param name="parents">Parent cancellation token sources. New cancellation token will be canceled if any of the parents is canceled.</param>
    member c.CreateCancellationTokenSource ([<O;D(null:obj)>] ?parents : seq<ICloudCancellationToken>) : ICloudCancellationTokenSource =
        async {
            let parents = parents |> Option.map Seq.toArray
            let! dcts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, ?parents = parents, elevate = true)
            return dcts :> ICloudCancellationTokenSource
        } |> Async.RunSync

    /// <summary>
    ///     Asynchronously submits supplied cloud computation for execution in the current MBrace runtime.
    ///     Returns an instance of CloudProcess, which can be queried for information on the progress of the computation.
    /// </summary>
    /// <param name="workflow">Cloud computation to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault retry policy for the process. Defaults to the client FaultPolicy property.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="additionalResources">Additional per-cloud process MBrace resources that can be appended to the computation state.</param>
    /// <param name="taskName">User-specified process name.</param>
    member c.CreateProcessAsync(workflow : Cloud<'T>, [<O;D(null:obj)>] ?cancellationToken : ICloudCancellationToken, 
                                    [<O;D(null:obj)>] ?faultPolicy : FaultPolicy, [<O;D(null:obj)>] ?target : IWorkerRef, 
                                    [<O;D(null:obj)>] ?additionalResources : ResourceRegistry, [<O;D(null:obj)>] ?taskName : string) : Async<CloudProcess<'T>> = async {
        let faultPolicy = defaultArg faultPolicy defaultFaultPolicy
        let dependencies = runtime.AssemblyManager.ComputeDependencies((workflow, faultPolicy))
        let assemblyIds = dependencies |> Array.map (fun d -> d.Id)
        do! runtime.AssemblyManager.UploadAssemblies(dependencies)
        if workers.Value.Length = 0 then
            runtime.SystemLogger.Logf LogLevel.Warning "No worker instances currently associated with cluster. Computation may never complete."

        return! Combinators.runStartAsCloudProcess runtime None assemblyIds taskName faultPolicy cancellationToken additionalResources target workflow
    }

    /// <summary>
    ///     Submits supplied cloud computation for execution in the current MBrace runtime.
    ///     Returns an instance of CloudProcess, which can be queried for information on the progress of the computation.
    /// </summary>
    /// <param name="workflow">Cloud computation to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault retry policy for the process. Defaults to the client FaultPolicy property.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="additionalResources">Additional per-cloud process MBrace resources that can be appended to the computation state.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.CreateProcess(workflow : Cloud<'T>, [<O;D(null:obj)>] ?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy, 
                                [<O;D(null:obj)>] ?target : IWorkerRef, [<O;D(null:obj)>] ?additionalResources : ResourceRegistry, [<O;D(null:obj)>] ?taskName : string) : CloudProcess<'T> =
        __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, 
                                    ?target = target, ?additionalResources = additionalResources, ?taskName = taskName) |> Async.RunSync


    /// <summary>
    ///     Asynchronously submits a cloud computation for execution in the current MBrace runtime
    ///     and waits until the computation completes with a value or fails with an exception.
    /// </summary>
    /// <param name="workflow">Cloud computation to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault retry policy for the process. Defaults to the client FaultPolicy property.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="additionalResources">Additional per-cloud process MBrace resources that can be appended to the computation state.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.RunAsync(workflow : Cloud<'T>, [<O;D(null:obj)>] ?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy, [<O;D(null:obj)>] ?additionalResources : ResourceRegistry, [<O;D(null:obj)>] ?target : IWorkerRef, [<O;D(null:obj)>] ?taskName : string) : Async<'T> = async {
        let! cloudProcess = __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?additionalResources = additionalResources, ?taskName = taskName)
        return! cloudProcess.AwaitResult()
    }

    /// <summary>
    ///     Submits a cloud computation for execution in the current MBrace runtime
    ///     and waits until the computation completes with a value or fails with an exception.
    /// </summary>
    /// <param name="workflow">Cloud computation to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault retry policy for the process. Defaults to the client FaultPolicy property.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="additionalResources">Additional per-cloud process MBrace resources that can be appended to the computation state.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.Run(workflow : Cloud<'T>, [<O;D(null:obj)>] ?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>] ?faultPolicy : FaultPolicy, [<O;D(null:obj)>] ?target : IWorkerRef, [<O;D(null:obj)>] ?additionalResources : ResourceRegistry, [<O;D(null:obj)>] ?taskName : string) : 'T =
        __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?additionalResources = additionalResources, ?taskName = taskName) |> Async.RunSync


    /// Gets a collection of all running or completed cloud processes that exist in the current MBrace runtime.
    member __.GetAllProcesses () : CloudProcess [] = taskManagerClient.GetAllProcesses() |> Async.RunSync

    /// <summary>
    ///     Attempts to get a cloud process instance using supplied identifier.
    /// </summary>
    /// <param name="id">Input cloud process identifier.</param>
    member __.TryGetProcessById(processId:string) = taskManagerClient.TryGetProcessById(processId) |> Async.RunSync

    /// <summary>
    ///     Looks up a CloudProcess instance from cluster using supplied identifier.
    /// </summary>
    /// <param name="processId">Input cloud process identifier.</param>
    member __.GetProcessById(processId:string) = 
        match __.TryGetProcessById processId with
        | None -> raise <| invalidArg "processId" "No cloud process with supplied id could be found in cluster."
        | Some t -> t

    /// <summary>
    ///     Deletes cloud process and all related data from MBrace cluster.
    /// </summary>
    /// <param name="cloud process">Cloud process to be cleared.</param>
    member __.ClearProcess(cloudProcess:CloudProcess<'T>) : unit = taskManagerClient.ClearProcess(cloudProcess) |> Async.RunSync

    /// <summary>
    ///     Deletes *all* cloud processes and related data from MBrace cluster.
    /// </summary>
    member __.ClearAllProcesses() : unit = taskManagerClient.ClearAllProcesses() |> Async.RunSync

    /// Gets a printed report of all current cloud processes.
    member __.FormatProcesses() : string = taskManagerClient.FormatProcesses()

    /// Prints a report of all current cloud processes to stdout.
    member __.ShowProcesses() : unit = taskManagerClient.ShowProcesses()

    /// Gets a client object that can be used for interoperating with the MBrace store.
    member __.Store : CloudStoreClient = storeClient

    /// Gets all available workers for the MBrace runtime.
    member __.Workers : WorkerRef [] = workers.Value

    /// Gets or sets the default fault policy used by computations
    /// uploaded by this client instance.
    member __.FaultPolicy
        with get () = defaultFaultPolicy
        and set fp  = defaultFaultPolicy <- fp

    /// Gets a printed report on all workers on the runtime
    member __.FormatWorkers() = WorkerReporter.Report(__.Workers, title = "Workers", borders = false)

    /// Prints a report on all workers on the runtime to stdout
    member __.ShowWorkers() = System.Console.WriteLine(__.FormatWorkers())

    /// Resolves runtime resource of given type
    member __.GetResource<'TResource> () : 'TResource = runtime.ResourceRegistry.Resolve<'TResource> ()

    /// <summary>
    ///     Asynchronously executes supplied cloud computation within the current, client process.
    ///     Parallelism is afforded through the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Cloud computation to execute.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics for local parallelism. Defaults to shared memory.</param>
    member __.RunLocallyAsync(workflow : Cloud<'T>, [<O;D(null:obj)>] ?memoryEmulation : MemoryEmulation) : Async<'T> =
        imem.ToAsync(workflow, ?memoryEmulation = memoryEmulation)

    /// <summary>
    ///     Asynchronously executes supplied cloud computation within the current, client process.
    ///     Parallelism is afforded through the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Cloud computation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics for local parallelism. Defaults to shared memory.</param>
    member __.RunLocally(workflow : Cloud<'T>, [<O;D(null:obj)>] ?cancellationToken : ICloudCancellationToken, [<O;D(null:obj)>] ?memoryEmulation : MemoryEmulation) : 'T = 
        imem.RunSynchronously(workflow, ?cancellationToken = cancellationToken, ?memoryEmulation = memoryEmulation)

    /// <summary>
    ///     Attaches user-supplied logger to client instance.
    ///     Returns an unsubscribe token if successful.
    /// </summary>
    /// <param name="logger">Logger instance to be attached.</param>
    member __.AttachLogger(logger : ISystemLogger) : System.IDisposable = runtime.LocalSystemLogManager.AttachLogger logger

    /// Gets or sets the system log level used by the client process.
    member __.LogLevel
        with get () = runtime.LocalSystemLogManager.LogLevel
        and set l = runtime.LocalSystemLogManager.LogLevel <- l

    /// <summary>
    ///     Asynchronously fetches all system logs generated by all workers in the MBrace runtime.
    /// </summary>
    /// <param name="worker">Specific worker to fetch logs for. Defaults to all workers.</param>
    /// <param name="logLevel">Maximum log level to display. Defaults to LogLevel.Info.</param>
    /// <param name="filter">User-specified log filtering function.</param>
    member __.GetSystemLogsAsync([<O;D(null:obj)>] ?worker : IWorkerRef, [<O;D(null:obj)>] ?logLevel : LogLevel, [<O;D(null:obj)>] ?filter : SystemLogEntry -> bool) = async {
        match worker with
        | Some(:? WorkerRef as w) -> return! w.GetSystemLogsAsync(?logLevel = logLevel, ?filter = filter)
        | Some _ -> return raise <| invalidArg "worker" "Invalid WorkerRef argument."
        | None ->
            let filter = defaultArg filter (fun _ -> true)
            let logLevel = defaultArg logLevel LogLevel.Info
            let! entries = runtime.RuntimeSystemLogManager.GetRuntimeLogs()
            return entries |> Seq.filter (fun e -> e.LogLevel <= logLevel && filter e) |> Seq.toArray
    }

    /// <summary>
    ///     Fetches all system logs generated by all workers in the MBrace runtime.
    /// </summary>
    /// <param name="worker">Specific worker to fetch logs for. Defaults to all workers.</param>
    /// <param name="logLevel">Maximum log level to display. Defaults to LogLevel.Info.</param>
    /// <param name="filter">User-specified log filtering function.</param>
    member __.GetSystemLogs([<O;D(null:obj)>] ?worker : IWorkerRef, [<O;D(null:obj)>] ?logLevel : LogLevel, [<O;D(null:obj)>] ?filter : SystemLogEntry -> bool) : SystemLogEntry[] =
        __.GetSystemLogsAsync(?worker = worker, ?logLevel = logLevel, ?filter = filter) |> Async.RunSync

    /// <summary>
    ///     Prints all system logs generated by all workers in the cluster to stdout.
    /// </summary>
    /// <param name="worker">Specific worker to fetch logs for. Defaults to all workers.</param>
    /// <param name="logLevel">Maximum log level to display. Defaults to LogLevel.Info.</param>
    /// <param name="filter">User-specified log filtering function.</param>
    member __.ShowSystemLogs([<O;D(null:obj)>] ?worker : IWorkerRef, [<O;D(null:obj)>] ?logLevel : LogLevel, [<O;D(null:obj)>] ?filter : SystemLogEntry -> bool) : unit =
        match worker with
        | Some(:? WorkerRef as w) -> w.ShowSystemLogs(?logLevel = logLevel, ?filter = filter)
        | Some _ -> invalidArg "worker" "Invalid WorkerRef argument."
        | None ->
            let filter = defaultArg filter (fun _ -> true)
            let logLevel = defaultArg logLevel LogLevel.Info
            runtime.RuntimeSystemLogManager.GetRuntimeLogs()
            |> Async.RunSync
            |> Seq.filter (fun e -> e.LogLevel <= logLevel && filter e)
            |> Seq.map (fun e -> SystemLogEntry.Format(e, showDate = true, showSourceId = true))
            |> Seq.iter System.Console.WriteLine

    /// Asynchronously clears all system logs from the cluster state.
    member __.ClearSystemLogsAsync() =
        runtime.RuntimeSystemLogManager.ClearLogs()

    /// Clears all system logs from the cluster state.
    member __.ClearSystemLogs() = 
        __.ClearSystemLogsAsync() |> Async.RunSync

    /// Event for subscribing to runtime-wide system logs
    [<CLIEvent>]
    member __.SystemLogs = getSystemLogPoller() :> IEvent<SystemLogEntry>

    /// <summary>
    ///     Registers a native assembly dependency to client state.
    /// </summary>
    /// <param name="assemblyPath">Path to native assembly.</param>
    member __.RegisterNativeDependency(assemblyPath : string) : unit =
        ignore <| runtime.AssemblyManager.RegisterNativeDependency assemblyPath

    /// Gets native assembly dependencies registered to client state.
    member __.NativeDependencies : string [] =
        runtime.AssemblyManager.NativeDependencies |> Array.map (fun v -> v.Image)

    /// Resets cluster state. This will cancel and delete all cloud process data.
    member __.ResetAsync() = runtime.ResetClusterState()

    /// Resets cluster state. This will cancel and delete all cloud process data.
    member __.Reset() = __.ResetAsync() |> Async.RunSync