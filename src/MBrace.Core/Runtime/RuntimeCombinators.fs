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
    static member CancellationToken = 
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx.CancellationToken)

    /// <summary>
    ///     Raise an exception.
    /// </summary>
    /// <param name="e">exception to be raised.</param>
    static member Raise<'T> (e : exn) : Cloud<'T> = raiseM e

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Workflow to be protected.</param>
    static member Catch(cloudWorkflow : Cloud<'T>) : Cloud<Choice<'T, exn>> = cloud {
        try
            let! res = cloudWorkflow
            return Choice1Of2 res
        with e ->
            return Choice2Of2 e
    }

    /// <summary>
    ///     Creates a cloud workflow that asynchronously sleeps for a given amount of time.
    /// </summary>
    /// <param name="millisecondsDue">Milliseconds to suspend computation.</param>
    static member Sleep(millisecondsDue : int) : Cloud<unit> = 
        Cloud.OfAsync<unit>(Async.Sleep millisecondsDue)

    /// <summary>
    ///     Wraps an asynchronous workflow into a cloud workflow.
    /// </summary>
    /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
    static member OfAsync<'T>(asyncWorkflow : Async<'T>) : Cloud<'T> = ofAsync asyncWorkflow

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : Cloud<'T>) : Cloud<unit> = cloud { let! _ = workflow in return () }

    /// <summary>
    ///     Disposes of a distributed resource.
    /// </summary>
    /// <param name="disposable">Resource to be disposed.</param>
    static member Dispose<'Disposable when 'Disposable :> ICloudDisposable>(disposable : 'Disposable) : Cloud<unit> =
        dispose disposable

    /// <summary>
    ///     Asynchronously await task completion
    /// </summary>
    /// <param name="task">Task to be awaited</param>
    static member AwaitTask (task : Task<'T>) : Cloud<'T> = 
        Cloud.OfAsync(Async.AwaitTask task)


    // region : runtime API

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Log(logEntry : string) : Cloud<unit> = cloud {
        let! runtime = Cloud.TryGetResource<ICloudRuntimeProvider> ()
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

    /// <summary>
    ///     Creates a cloud computation that will execute the given computations
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Cloud<'T>>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun c -> c,None))
        return! Cloud.WithAppendedStackTrace "Cloud.Parallel[T](seq<Cloud<T>> computations)" workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute provided computation on every available worker
    ///     in the cluster and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Any exception raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computation">Computation to be executed in every worker.</param>
    static member Parallel(computation : Cloud<'T>) = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleParallel (workers |> Seq.map (fun w -> computation, Some w))
        return! Cloud.WithAppendedStackTrace "Cloud.EveryWhere[T](Cloud<T> computation)" workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Cloud<'T> * IWorkerRef>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun (c,w) -> c,Some w))
        return! Cloud.WithAppendedStackTrace "Cloud.Parallel[T](seq<Cloud<T> * IWorkerRef> computations)" workflow
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
    static member Choice (computations : seq<Cloud<'T option>>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun c -> c,None))
        return! Cloud.WithAppendedStackTrace "Cloud.Choice[T](seq<Cloud<T option>> computations)" workflow
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
    static member Choice (computation : Cloud<'T option>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleChoice (workers |> Seq.map (fun w -> computation,Some w))
        return! Cloud.WithAppendedStackTrace "Cloud.Choice[T](Cloud<T option> computation)" workflow
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
    static member Choice (computations : seq<Cloud<'T option> * IWorkerRef>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun (c,w) -> c,Some w))
        return! Cloud.WithAppendedStackTrace "Cloud.Choice[T](seq<Cloud<T option> * IWorkerRef> computations)" workflow
    }

    /// <summary>
    ///     Start cloud computation as child. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds; defaults to infinite.</param>
    static member StartChild(computation : Cloud<'T>, ?target : IWorkerRef, ?timeoutMilliseconds:int) : Cloud<Cloud<'T>> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let! receiver = runtime.ScheduleStartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
        return Cloud.WithAppendedStackTrace "Cloud.StartChild[T](Cloud<T> computation)" receiver
    }

    /// <summary>
    ///     Try/Finally combinator for monadic finalizers.
    /// </summary>
    /// <param name="body">Workflow body.</param>
    /// <param name="finalizer">Finalizer workflow.</param>
    static member TryFinally(body : Cloud<'T>, finalizer : Cloud<unit>) : Cloud<'T> =
        tryFinally body finalizer

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : Cloud<IWorkerRef> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : Cloud<IWorkerRef []> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.GetAvailableWorkers()
    }

    /// <summary>
    ///     Gets total number of available workers in cluster context.
    /// </summary>
    static member GetWorkerCount () : Cloud<int> = cloud {
        let! workers = Cloud.GetAvailableWorkers()
        return workers.Length
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud process.
    /// </summary>
    static member GetProcessId () : Cloud<string> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return runtime.ProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud task.
    /// </summary>
    static member GetTaskId () : Cloud<string> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return runtime.TaskId
    }

    /// <summary>
    ///     Gets the current scheduling context for the workflow
    ///     which indicates if parallel jobs are scheduled using
    ///     Distribution, Thread parallelism or Sequentially.
    /// </summary>
    static member GetSchedulingContext () = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return runtime.SchedulingContext
    }

    /// <summary>
    ///     Sets a new scheduling context for target workflow.
    /// </summary>
    /// <param name="workflow">Target workflow.</param>
    /// <param name="schedulingContext">Target scheduling context.</param>
    [<CompilerMessage("'SetSchedulingContext' only intended for runtime implementers.", 444)>]
    static member WithSchedulingContext (schedulingContext : SchedulingContext) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider>()
        let runtime' = runtime.WithSchedulingContext schedulingContext
        return! Cloud.SetResource(workflow, runtime')
    }

    /// <summary>
    ///     Force thread local execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToLocal(workflow : Cloud<'T>) = Cloud.WithSchedulingContext ThreadParallel workflow

    /// <summary>
    ///     Force sequential execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToSequential(workflow : Cloud<'T>) = Cloud.WithSchedulingContext Sequential workflow

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    static member GetFaultPolicy () : Cloud<FaultPolicy> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<ICloudRuntimeProvider> ()
        let runtime' = runtime.WithFaultPolicy policy
        return! Cloud.SetResource(workflow, runtime')
    }