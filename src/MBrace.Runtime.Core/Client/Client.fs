namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Client

open MBrace.Runtime.Utils

/// MBrace Sample runtime client instance.
[<AbstractClass>]
type MBraceClient (resources : IRuntimeManager) =

    let imem = LocalRuntime.Create(resources = resources.ResourceRegistry)
    let processManager = new CloudProcessManager(resources)
    let workerCache = CacheAtom.Create(async { return! resources.WorkerManager.GetAvailableWorkers() }, intervalMilliseconds = 500)

    /// Creates a fresh cloud cancellation token source for this runtime
    member c.CreateCancellationTokenSource (?parents : seq<ICloudCancellationToken>) : ICloudCancellationTokenSource =
        async {
            let parents = parents |> Option.map Seq.toArray
            let! dcts = DistributedCancellationToken.Create(resources.CancellationEntryFactory, ?parents = parents, elevate = true)
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
                                ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : Async<CloudProcess<'T>> = async {

        let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.Retry(maxRetries = 1)
        let dependencies = resources.AssemblyManager.ComputeDependencies((workflow, faultPolicy))
        let assemblyIds = dependencies |> Array.map (fun d -> d.Id)
        do! resources.AssemblyManager.UploadAssemblies(dependencies)
        let! tcs = Combinators.runStartAsCloudTask resources assemblyIds taskName faultPolicy cancellationToken target workflow
        return processManager.GetProcess tcs
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime as task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.CreateProcess(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) : CloudProcess<'T> =
        __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync


    /// <summary>
    ///     Asynchronously executes a workflow on the distributed runtime.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) = async {
        let! task = __.CreateProcessAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName)
        return task.Result
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime synchronously
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    /// <param name="taskName">User-specified process name.</param>
    member __.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?taskName : string) =
        __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target, ?taskName = taskName) |> Async.RunSync

    /// Gets all processes of provided cluster
    member __.GetAllProcesses () = processManager.GetAllProcesses() |> Async.RunSync

    /// <summary>
    ///     Gets process object by process id.
    /// </summary>
    /// <param name="id">Task id.</param>
    member __.GetProcessById(id:string) = processManager.GetProcessById(id) |> Async.RunSync

    /// <summary>
    ///     Clear cluster data for provided process.
    /// </summary>
    /// <param name="process">Process to be cleared.</param>
    member __.ClearProcess(p:CloudProcess) = processManager.ClearProcess(p) |> Async.RunSync

    /// <summary>
    ///     Clear all process data from cluster.
    /// </summary>
    member __.ClearAllProcesses() = processManager.ClearAllProcesses() |> Async.RunSync

    /// <summary>
    ///     Run workflow as local, in-memory computation
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    member __.RunLocallyAsync(workflow : Cloud<'T>) : Async<'T> = imem.RunAsync workflow

    /// Returns the store client for provided runtime.
    member __.StoreClient = imem.StoreClient

    /// Gets all available workers for current runtime.
    member __.Workers = workerCache.Value |> Array.map (fun (w,_,_) -> w)

    /// Gets worker info for all workers in runtime.
    member __.GetWorkerInfo () = workerCache.Value

    /// <summary>
    ///     Gets node performance info for supplied worker.
    /// </summary>
    /// <param name="worker">Worker to be evaluated.</param>
    member __.GetPerformanceInfo(worker : IWorkerRef) = 
        let _,_,p = workerCache.Value |> Array.find (fun (w,_,_) -> w = worker)
        p

    /// <summary>
    ///     Run workflow as local, in-memory computation
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member __.RunLocally(workflow, ?cancellationToken) : 'T = imem.Run(workflow, ?cancellationToken = cancellationToken)