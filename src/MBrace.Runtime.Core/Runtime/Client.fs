namespace MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Client

/// MBrace Sample runtime client instance.
[<AbstractClass>]
type MBraceClient () as self =

    let imem = lazy(LocalRuntime.Create(resources = self.Resources.ResourceRegistry))

    abstract Resources : IRuntimeResourceManager

    /// Creates a fresh cloud cancellation token source for this runtime
    member c.CreateCancellationTokenSource (?parents : seq<ICloudCancellationToken>) : ICloudCancellationTokenSource =
        async {
            let parents = parents |> Option.map Seq.toArray
            let! dcts = DistributedCancellationToken.Create(c.Resources.CancellationEntryFactory, ?parents = parents, elevate = true)
            return dcts :> ICloudCancellationTokenSource
        } |> Async.RunSync

    /// <summary>
    ///     Asynchronously execute a workflow on the distributed runtime as task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    member c.StartAsTaskAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, 
                                ?faultPolicy : FaultPolicy, ?target : IWorkerRef) : Async<ICloudTask<'T>> = async {

        let faultPolicy = match faultPolicy with Some fp -> fp | None -> FaultPolicy.Retry(maxRetries = 1)
        let processId = mkUUID()
        let dependencies = c.Resources.AssemblyManager.ComputeDependencies((workflow, faultPolicy))
        let assemblyIds = dependencies |> Array.map (fun d -> d.Id)
        do! c.Resources.AssemblyManager.UploadAssemblies(dependencies)
        let! tcs = Combinators.runStartAsCloudTask c.Resources assemblyIds processId faultPolicy cancellationToken target workflow
        return tcs.Task
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime as task.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    member __.StartAsTask(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef) : ICloudTask<'T> =
        __.StartAsTaskAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target) |> Async.RunSync


    /// <summary>
    ///     Asynchronously executes a workflow on the distributed runtime.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef) = async {
        let! task = __.StartAsTaskAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target)
        return task.Result
    }

    /// <summary>
    ///     Execute a workflow on the distributed runtime synchronously
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="cancellationToken">Cancellation token for computation.</param>
    /// <param name="faultPolicy">Fault policy. Defaults to single retry.</param>
    /// <param name="target">Target worker to initialize computation.</param>
    member __.Run(workflow : Cloud<'T>, ?cancellationToken : ICloudCancellationToken, ?faultPolicy : FaultPolicy, ?target : IWorkerRef) =
        __.RunAsync(workflow, ?cancellationToken = cancellationToken, ?faultPolicy = faultPolicy, ?target = target) |> Async.RunSync

    /// <summary>
    ///     Run workflow as local, in-memory computation
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    member __.RunLocallyAsync(workflow : Cloud<'T>) : Async<'T> = imem.Value.RunAsync workflow

    /// Returns the store client for provided runtime.
    member __.StoreClient = imem.Value.StoreClient

    /// Gets all available workers for current runtime.
    member __.Workers = __.Resources.GetAvailableWorkers() |> Async.RunSynchronously

    /// <summary>
    ///     Run workflow as local, in-memory computation
    /// </summary>
    /// <param name="workflow">Workflow to execute</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member __.RunLocally(workflow, ?cancellationToken) : 'T = imem.Value.Run(workflow, ?cancellationToken = cancellationToken)