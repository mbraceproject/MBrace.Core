namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.InMemoryRuntime

open MBrace.Runtime.Utils

/// MBrace Sample runtime client instance.
[<AbstractClass>]
type MBraceClient (runtime : IRuntimeManager) =

    let imem = InMemoryRuntime.Create(resources = runtime.ResourceRegistry)
    let taskManagerClient = new CloudTaskManagerClient(runtime)
    let getWorkers () = async {
        let! workers = runtime.WorkerManager.GetAvailableWorkers()
        return workers |> Array.map (fun w -> WorkerRef.Create(runtime, w.Id))
    }

    let workers = CacheAtom.Create(getWorkers(), intervalMilliseconds = 500)

    /// Creates a fresh cloud cancellation token source for this runtime
    member c.CreateCancellationTokenSource (?parents : seq<ICloudCancellationToken>) : ICloudCancellationTokenSource =
        async {
            let parents = parents |> Option.map Seq.toArray
            let! dcts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, ?parents = parents, elevate = true)
            return dcts :> ICloudCancellationTokenSource
        } |> Async.RunSync

    /// <summary>
    ///     Asynchronously execute a workflow on the distributed runtime as task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member c.CreateProcessAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, 
                                ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : Async<CloudTask<'T>> = async {

        let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.Retry(maxRetries = 1)
        let dependencies = runtime.AssemblyManager.ComputeDependencies((workflow, faultPolicy))
        let assemblyIds = dependencies |> Array.map (fun d -> d.Id)
        do! runtime.AssemblyManager.UploadAssemblies(dependencies)
        return! Combinators.runStartAsCloudTask runtime assemblyIds taskName faultPolicy cancellationToken target workflow
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime as task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.CreateProcess(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : CloudTask<'T> =
        __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync


    /// <summary>
    ///     Asynchronously executes a workflow on the distributed runtime.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : Async<'T> = async {
        let! task = __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName)
        return! task.AwaitResult()
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime synchronously
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : 'T =
        __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync

    /// Gets all processes of provided cluster
    member __.GetAllProcesses () = taskManagerClient.GetAllTasks() |> Async.RunSync

    /// <summary>
    ///     Gets process object by process id.
    /// </summary>
    /// <param name="id">Task id.</param>
    member __.TryGetProcessById(taskId:string) = taskManagerClient.TryGetTaskById(taskId) |> Async.RunSync

    /// <summary>
    ///     Clear cluster data for provided process.
    /// </summary>
    /// <param name="task">Process to be cleared.</param>
    member __.ClearProcess(task:CloudTask<'T>) = taskManagerClient.ClearTask(task) |> Async.RunSync

    /// <summary>
    ///     Clear all process data from cluster.
    /// </summary>
    member __.ClearAllProcesses() = taskManagerClient.ClearAllTasks() |> Async.RunSync

    /// Gets a printed report of all currently executing processes
    member __.GetProcessInfo() = taskManagerClient.GetProcessInfo()
    /// Prints a report of all currently executing processes to stdout
    member __.ShowProcessInfo() = taskManagerClient.ShowProcessInfo()

    /// <summary>
    ///     Run workflow as local, in-memory computation
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    member __.RunLocallyAsync(workflow : Cloud<'T>) : Async<'T> = imem.RunAsync workflow

    /// Returns the store client for provided runtime.
    member __.StoreClient = imem.StoreClient

    /// Gets all available workers for current runtime.
    member __.Workers = workers.Value

    /// Gets a printed report on all workers on the runtime
    member __.GetWorkerInfo () = WorkerReporter.Report(__.Workers, title = "Workers", borders = false)
    /// Prints a report on all workers on the runtime to stdout
    member __.ShowWorkerInfo () = System.Console.WriteLine(__.GetWorkerInfo())
    /// Resolves runtime resource of given type
    member __.GetResource<'TResource> () : 'TResource = runtime.ResourceRegistry.Resolve<'TResource> ()

    /// <summary>
    ///     Run workflow as local, in-memory computation.
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member __.RunLocally(workflow, ?cancellationToken) : 'T = imem.Run(workflow, ?cancellationToken = cancellationToken)