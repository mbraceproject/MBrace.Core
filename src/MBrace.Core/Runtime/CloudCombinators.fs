namespace MBrace

open System.Threading.Tasks

open MBrace.Continuation
open MBrace.Runtime

#nowarn "444"

/// Cloud workflows user API
type Cloud =

    // region : core continuation API
    
    /// <summary>
    ///     Gets the current cancellation token.
    /// </summary>
    static member CancellationToken : Local<ICloudCancellationToken> = 
        Body(fun ctx cont -> cont.Success ctx ctx.CancellationToken)

    /// <summary>
    ///     Raise an exception.
    /// </summary>
    /// <param name="e">exception to be raised.</param>
    static member Raise<'T> (e : exn) : Local<'T> = raiseM e

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Workflow to be protected.</param>
    static member Catch(workflow : Workflow<'C, 'T>) : Workflow<'C, Choice<'T, exn>> = wfb {
        try
            let! res = workflow
            return Choice1Of2 res
        with e ->
            return Choice2Of2 e
    }

    /// <summary>
    ///     Creates a cloud workflow that asynchronously sleeps for a given amount of time.
    /// </summary>
    /// <param name="millisecondsDue">Milliseconds to suspend computation.</param>
    static member Sleep(millisecondsDue : int) : Local<unit> = 
        Cloud.OfAsync<unit>(Async.Sleep millisecondsDue)

    /// <summary>
    ///     Wraps an asynchronous workflow into a cloud workflow.
    /// </summary>
    /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
    static member OfAsync<'T>(asyncWorkflow : Async<'T>) : Local<'T> = ofAsync asyncWorkflow

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : Workflow<'C, 'T>) : Workflow<'C, unit> = wfb { let! _ = workflow in return () }

    /// <summary>
    ///     Disposes of a distributed resource.
    /// </summary>
    /// <param name="disposable">Resource to be disposed.</param>
    static member Dispose<'Disposable when 'Disposable :> ICloudDisposable>(disposable : 'Disposable) : Local<unit> =
        dispose disposable

    /// <summary>
    ///     Asynchronously awaits a System.Threading.Task for completion.
    /// </summary>
    /// <param name="task">Awaited task.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    static member AwaitTask(task : Task<'T>, ?timeoutMilliseconds : int) : Local<'T> =
        Body(fun ctx cont ->
            let task = match timeoutMilliseconds with None -> task | Some ms -> task.WithTimeout ms
            let onCompletion (t : Task<'T>) =
                match t.Status with
                | TaskStatus.Faulted -> cont.Exception ctx (capture t.InnerException)
                | TaskStatus.Canceled -> cont.Cancellation ctx (new System.OperationCanceledException())
                | _ -> cont.Success ctx t.Result

            let _ = task.ContinueWith(onCompletion, TaskContinuationOptions.None) in ())

    // region : runtime API

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Log(logEntry : string) : Local<unit> = local {
        let! runtime = Workflow.TryGetResource<ICloudRuntimeProvider> ()
        return 
            match runtime with
            | None -> ()
            | Some r -> r.Logger.Log logEntry
    }

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Logf fmt = Printf.ksprintf Cloud.Log fmt

    /// Returns true iff runtime supports executing workflows in specific worker.
    /// Should be used with combinators that support worker targeting like Cloud.Parallel/Choice/StartChild.
    static member IsTargetedWorkerSupported : Local<bool> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider>()
        return runtime.IsTargetedWorkerSupported
    }

    /// <summary>
    ///     Creates a cloud computation that will execute the given computations
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<#Workflow<'T>>) : Cloud<'T []> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun c -> c,None))
        return! Workflow.WithAppendedStackTrace "Cloud.Parallel[T](seq<Cloud<T>> computations)" workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute provided computation on every available worker
    ///     in the cluster and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Any exception raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computation">Computation to be executed in every worker.</param>
    static member Parallel(computation : Workflow<'T>) : Cloud<'T []> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleParallel (workers |> Seq.map (fun w -> computation, Some w))
        return! Workflow.WithAppendedStackTrace "Cloud.Parallel[T](Cloud<T> computation)" workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<#Workflow<'T> * IWorkerRef>) : Cloud<'T []> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun (c,w) -> c,Some w))
        return! Workflow.WithAppendedStackTrace "Cloud.Parallel[T](seq<Cloud<T> * IWorkerRef> computations)" workflow
    }

    /// <summary>
    ///     Returns a cloud computation that will execute given computations
    ///     possibly in parallel and will return when any of the supplied computations
    ///     have returned a successful value or if all of them fail to succeed. 
    ///     If a computation succeeds the rest of them are canceled.
    ///     The success of a computation is encoded as an option type.
    ///     This operator may create distribution.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<#Workflow<'T option>>) : Cloud<'T option> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun c -> c,None))
        return! Workflow.WithAppendedStackTrace "Cloud.Choice[T](seq<Cloud<T option>> computations)" workflow
    }

    /// <summary>
    ///     Returns a cloud computation that will execute the given computation on every available worker
    ///     possibly in parallel and will return when any of the supplied computations
    ///     have returned a successful value or if all of them fail to succeed. 
    ///     If a computation succeeds the rest of them are canceled.
    ///     The success of a computation is encoded as an option type.
    ///     This operator may create distribution.
    /// </summary>
    /// <param name="computation">Input computation to be executed everywhere.</param>
    static member Choice (computation : Workflow<'T option>) : Cloud<'T option> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleChoice (workers |> Seq.map (fun w -> computation, Some w))
        return! Workflow.WithAppendedStackTrace "Cloud.Choice[T](Cloud<T option> computation)" workflow
    }

    /// <summary>
    ///     Returns a cloud computation that will execute the given computation on the corresponding worker,
    ///     possibly in parallel and will return when any of the supplied computations
    ///     have returned a successful value or if all of them fail to succeed. 
    ///     If a computation succeeds the rest of them are canceled.
    ///     The success of a computation is encoded as an option type.
    ///     This operator may create distribution.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<#Workflow<'T option> * IWorkerRef>) : Cloud<'T option> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun (c,w) -> c, Some w))
        return! Workflow.WithAppendedStackTrace "Cloud.Choice[T](seq<Cloud<T option> * IWorkerRef> computations)" workflow
    }

    /// <summary>
    ///     Start cloud computation as a task. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="cancellationToken">Cancellation token for task. Defaults to current cancellation token.</param>
    static member StartAsCloudTask(computation : Workflow<'T>, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?cancellationToken:ICloudCancellationToken) : Cloud<ICloudTask<'T>> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let! defaultToken = Cloud.CancellationToken
        let cancellationToken = defaultArg cancellationToken defaultToken
        let faultPolicy = defaultArg faultPolicy runtime.FaultPolicy
        return! runtime.ScheduleStartAsTask(computation, faultPolicy, cancellationToken, ?target = target)
    }

    /// <summary>
    ///     Asynchronously awaits cloud task for completion and returns its result.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite</param>
    static member AwaitCloudTask(task : ICloudTask<'T>, ?timeoutMilliseconds:int) : Local<'T> = local {
        return! task.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
    }

    /// <summary>
    ///     Start cloud computation as child process. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    static member StartChild(workflow : Workflow<'T>, ?target : IWorkerRef) : Cloud<Local<'T>> = cloud {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let! cancellationToken = Cloud.CancellationToken
        let! task = runtime.ScheduleStartAsTask(workflow, runtime.FaultPolicy, cancellationToken, ?target = target)
        return Workflow.WithAppendedStackTrace "Cloud.StartChild[T](Cloud<T> computation)" (local { return! task.AwaitResult() })
    }

    /// <summary>
    ///     Try/Finally combinator for monadic finalizers.
    /// </summary>
    /// <param name="body">Workflow body.</param>
    /// <param name="finalizer">Finalizer workflow.</param>
    static member TryFinally(body : Workflow<'C,'T>, finalizer : Local<unit>) : Workflow<'C, 'T> =
        tryFinally body finalizer

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : Local<IWorkerRef> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : Local<IWorkerRef []> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.GetAvailableWorkers()
    }

    /// <summary>
    ///     Gets total number of available workers in cluster context.
    /// </summary>
    static member GetWorkerCount () : Local<int> = local {
        let! workers = Cloud.GetAvailableWorkers()
        return workers.Length
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud process.
    /// </summary>
    static member GetProcessId () : Local<string> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return runtime.ProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud job.
    /// </summary>
    static member GetJobId () : Local<string> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return runtime.JobId
    }

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    static member GetFaultPolicy () : Local<FaultPolicy> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Workflow<'C, 'T>) : Workflow<'C, 'T> = wfb {
        let! runtime = plug <| Workflow.GetResource<ICloudRuntimeProvider> ()
        let runtime' = runtime.WithFaultPolicy policy
        return! Workflow.WithResource(workflow, runtime')
    }

    /// Creates a new cloud cancellation token source
    static member CreateCancellationTokenSource () = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [||]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="parent">Parent cancellation token. Defaults to the current process cancellation token.</param>
    static member CreateLinkedCancellationTokenSource(?parent : ICloudCancellationToken) = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let! currentCt = Cloud.CancellationToken
        let parent = defaultArg parent currentCt
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [| parent |]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="token1">First parent cancellation token.</param>
    /// <param name="token2">Second parent cancellation token.</param>s
    static member CreateLinkedCancellationTokenSource(token1 : ICloudCancellationToken, token2 : ICloudCancellationToken) = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [|token1 ; token2|]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="tokens">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource (Seq.toArray tokens)
    }


/// Local-parallelism combinators
type Local =

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Local<'T>>) : Local<'T []> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleLocalParallel computations
        return! Workflow.WithAppendedStackTrace "Local.Parallel[T](seq<Local<T>> computations)" workflow
    }

    /// <summary>
    ///     Returns a cloud computation that will execute given computations
    ///     possibly in parallel and will return when any of the supplied computations
    ///     have returned a successful value or if all of them fail to succeed. 
    ///     If a computation succeeds the rest of them are canceled.
    ///     The success of a computation is encoded as an option type.
    ///     This operator may create distribution.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<Local<'T option>>) : Local<'T option> = local {
        let! runtime = Workflow.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleLocalChoice computations
        return! Workflow.WithAppendedStackTrace "Local.Choice[T](seq<Local<T option>> computations)" workflow
    }