namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.ThreadPool
open MBrace.Runtime.Utils

/// MBrace runtime client handle abstract class.
[<AbstractClass>]
type MBraceClient (runtime : IRuntimeManager) =

    let checkVagabondDependencies (graph:obj) = runtime.AssemblyManager.ComputeDependencies graph |> ignore
    let imem = ThreadPoolClient.Create(resources = runtime.ResourceRegistry, memoryEmulation = MemoryEmulation.Shared, vagabondChecker = checkVagabondDependencies)
    let storeClient = CloudStoreClient.Create(imem)

    let taskManagerClient = new CloudTaskManagerClient(runtime)
    let getWorkers () = async {
        let! workers = runtime.WorkerManager.GetAvailableWorkers()
        return workers |> Array.map (fun w -> WorkerRef.Create(runtime, w.Id))
    }

    let workers = CacheAtom.Create(getWorkers(), intervalMilliseconds = 500)

    /// <summary>
    ///     Creates a fresh cloud cancellation token source for use in the MBrace cluster.
    /// </summary>
    /// <param name="parents">Parent cancellation token sources. New cancellation token will be canceled if any of the parents is canceled.</param>
    member c.CreateCancellationTokenSource (?parents : seq<ICloudCancellationToken>) : ICloudCancellationTokenSource =
        async {
            let parents = parents |> Option.map Seq.toArray
            let! dcts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, ?parents = parents, elevate = true)
            return dcts :> ICloudCancellationTokenSource
        } |> Async.RunSync

    /// <summary>
    ///     Asynchronously submits supplied cloud workflow for execution in the current MBrace runtime.
    ///     Returns an instance of CloudTask, which can be queried for information on the progress of the computation.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member c.CreateCloudTaskAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, 
                                    ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : Async<CloudTask<'T>> = async {

        let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.Retry(maxRetries = 1)
        let dependencies = runtime.AssemblyManager.ComputeDependencies((workflow, faultPolicy))
        let assemblyIds = dependencies |> Array.map (fun d -> d.Id)
        do! runtime.AssemblyManager.UploadAssemblies(dependencies)
        return! Combinators.runStartAsCloudTask runtime true assemblyIds taskName faultPolicy cancellationToken target workflow
    }

    /// <summary>
    ///     Submits supplied cloud workflow for execution in the current MBrace runtime.
    ///     Returns an instance of CloudTask, which can be queried for information on the progress of the computation.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.CreateCloudTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : CloudTask<'T> =
        __.CreateCloudTaskAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync


    /// <summary>
    ///     Asynchronously submits a cloud workflow for execution in the current MBrace runtime
    ///     and waits until the computation completes with a value or fails with an exception.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.RunOnCloudAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : Async<'T> = async {
        let! task = __.CreateCloudTaskAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName)
        return! task.AwaitResult()
    }

    /// <summary>
    ///     Submits a cloud workflow for execution in the current MBrace runtime
    ///     and waits until the computation completes with a value or fails with an exception.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.RunOnCloud(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : 'T =
        __.RunOnCloudAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync

    /// Gets a collection of all running or completed cloud tasks that exist in the current MBrace runtime.
    member __.GetAllCloudTasks () : CloudTask [] = taskManagerClient.GetAllTasks() |> Async.RunSync

    /// <summary>
    ///     Attempts to get a Cloud task instance using supplied identifier.
    /// </summary>
    /// <param name="id">Input task identifier.</param>
    member __.TryGetCloudTaskById(taskId:string) = taskManagerClient.TryGetTaskById(taskId) |> Async.RunSync

    /// <summary>
    ///     Deletes cloud task and all related data from MBrace cluster.
    /// </summary>
    /// <param name="task">Cloud task to be cleared.</param>
    member __.ClearCloudTask(task:CloudTask<'T>) : unit = taskManagerClient.ClearTask(task) |> Async.RunSync

    /// <summary>
    ///     Deletes *all* cloud tasks and related data from MBrace cluster.
    /// </summary>
    member __.ClearAllCloudTasks() : unit = taskManagerClient.ClearAllTasks() |> Async.RunSync

    /// Gets a printed report of all current cloud tasks.
    member __.GetCloudTaskInfo() : string = taskManagerClient.GetTaskInfo()
    /// Prints a report of all current cloud tasks to stdout.
    member __.ShowCloudTaskInfo() : unit = taskManagerClient.ShowTaskInfo()

    /// Gets a client object that can be used for interoperating with the MBrace store.
    member __.Store : CloudStoreClient = storeClient

    /// Gets all available workers for the MBrace runtime.
    member __.Workers : WorkerRef [] = workers.Value

    /// Gets a printed report on all workers on the runtime
    member __.GetWorkerInfo () = WorkerReporter.Report(__.Workers, title = "Workers", borders = false)
    /// Prints a report on all workers on the runtime to stdout
    member __.ShowWorkerInfo () = System.Console.WriteLine(__.GetWorkerInfo())
    /// Resolves runtime resource of given type
    member __.GetResource<'TResource> () : 'TResource = runtime.ResourceRegistry.Resolve<'TResource> ()

    /// <summary>
    ///     Asynchronously executes supplied cloud workflow within the current, client process.
    ///     Parallelism is afforded through the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Cloud workflow to execute.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics for local parallelism. Defaults to shared memory.</param>
    member __.RunOnCurrentProcessAsync(workflow : Cloud<'T>, ?memoryEmulation : MemoryEmulation) : Async<'T> =
        imem.ToAsync(workflow, ?memoryEmulation = memoryEmulation)

    /// <summary>
    ///     Asynchronously executes supplied cloud workflow within the current, client process.
    ///     Parallelism is afforded through the .NET thread pool.
    /// </summary>
    /// <param name="workflow">Cloud workflow to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="memoryEmulation">Specify memory emulation semantics for local parallelism. Defaults to shared memory.</param>
    member __.RunOnCurrentProcess(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?memoryEmulation : MemoryEmulation) : 'T = 
        imem.RunSynchronously(workflow, ?cancellationToken = cancellationToken, ?memoryEmulation = memoryEmulation)

    /// <summary>
    ///     Attaches user-supplied logger to client instance.
    ///     Returns an unsubscribe token if successful.
    /// </summary>
    /// <param name="logger">Logger instance to be attached.</param>
    member __.AttachLogger(logger : ISystemLogger) : System.IDisposable = runtime.AttachSystemLogger logger

    /// Gets or sets the system log level used by the client process.
    member __.LogLevel
        with get () = runtime.LogLevel
        and set l = runtime.LogLevel <- l