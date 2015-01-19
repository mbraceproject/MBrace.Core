namespace MBrace.Continuation

open System.Threading
open System.Threading.Tasks

open MBrace
open MBrace.Continuation

#nowarn "444"

/// Cloud workflows continuation API
type Cloud =
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : Cloud<'T> =
        fromContinuations body

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <paran name"continuation">Root continuation for workflow.</param>
    /// <param name="context">Local execution context.</param>
    static member StartWithContinuations(cloudWorkflow : Cloud<'T>, continuation : Continuation<'T>, ?context : ExecutionContext) : unit =
        let context = match context with None -> ExecutionContext.Empty() | Some ctx -> ctx
        let (Body f) = cloudWorkflow in f context continuation

    /// <summary>
    ///     Returns the resource registry for current execution context.
    /// </summary>
    [<CompilerMessage("'GetResourceRegistry' only intended for runtime implementers.", 444)>]
    static member GetResourceRegistry () : Cloud<ResourceRegistry> = getResources ()

    /// <summary>
    ///     Gets resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResource' only intended for runtime implementers.", 444)>]
    static member GetResource<'TResource> () : Cloud<'TResource> =
        Cloud.FromContinuations(fun ctx cont ->
            let res = protect (fun () -> ctx.Resources.Resolve<'TResource> ()) ()
            cont.Choice(ctx, res))

    /// <summary>
    ///     Try Getting resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member TryGetResource<'TResource> () : Cloud<'TResource option> =
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx <| ctx.Resources.TryResolve<'TResource> ())

    /// <summary>
    ///     Installs a new resource to executed workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="resource">Resource to be installed.</param>
    [<CompilerMessage("'SetResource' only intended for runtime implementers.", 444)>]
    static member SetResource(workflow : Cloud<'T>, resource : 'Resource) : Cloud<'T> =
        Cloud.FromContinuations(fun ctx cont -> let (Body f) = workflow in f { ctx with Resources = ctx.Resources.Register resource } cont)

    /// <summary>
    ///     Wraps a cloud workflow into an asynchronous workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource resolver to be used; defaults to empty resource registry.</param>
    static member ToAsync(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry) : Async<'T> =
        let resources = match resources with Some r -> r | None -> ResourceRegistry.Empty
        toAsync resources cloudWorkflow

    /// <summary>
    ///     Wraps a workflow with a mapped continuation.
    /// </summary>
    /// <param name="mapper">Continuation mapping function.</param>
    /// <param name="workflow">Input workflow.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithMappedContinuation (mapper : Continuation<'T> -> Continuation<'S>) (workflow : Cloud<'S>) =
        Body(fun ctx cont -> let (Body f) = workflow in f ctx (mapper cont))

    /// <summary>
    ///     Appends a function information entry to the symbolic stacktrace in the exception continuation.
    /// </summary>
    /// <param name="functionName">Function info string to be appended.</param>
    /// <param name="workflow">Workflow to be wrapped.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithAppendedStackTrace (functionName : string) (workflow : Cloud<'T>) =
        Body(fun ctx cont ->
            let cont' = { cont with Exception = fun ctx edi -> cont.Exception ctx (appendToStacktrace functionName edi) }
            let (Body f) = workflow
            f ctx cont')

    /// <summary>
    ///     Starts provided cloud workflow in the thread pool.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context.</param>
    /// <param name="cancellationToken">Local Cancellation token.</param>
    static member Start(cloudWorkflow : Cloud<unit>, ?resources, ?cancellationToken) : unit =
        let asyncWorkflow = Cloud.ToAsync(cloudWorkflow, ?resources = resources)
        Async.Start(asyncWorkflow, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Starts given workflow as a separate, locally executing task.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry used with workflows.</param>
    /// <param name="taskCreationOptions">Resource registry used with workflows.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    static member StartAsTask(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry, 
                                ?taskCreationOptions : TaskCreationOptions, ?cancellationToken : CancellationToken) : Task<'T> =

        let asyncWorkflow = Cloud.ToAsync(cloudWorkflow, ?resources = resources)
        Async.StartAsTask(asyncWorkflow, ?taskCreationOptions = taskCreationOptions, ?cancellationToken = cancellationToken)

    /// <summary>
    ///     Synchronously await a locally executing workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
    /// <param name="cancellationToken">Cancellation token to be used.</param>
    static member RunSynchronously(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry, ?cancellationToken) : 'T =
        let wf = Cloud.ToAsync(cloudWorkflow, ?resources = resources) 
        Async.RunSync(wf, ?cancellationToken = cancellationToken)


//    static member RunLocal(cloudWorkflow : Cloud<'T>, ?)


namespace MBrace

open System.Threading.Tasks
open MBrace.Continuation

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
        cloud { return! disposable.Dispose() }

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
        let! runtime = Cloud.TryGetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleChoice (workers |> Seq.map (fun w -> computation,Some w))
        return! Cloud.WithAppendedStackTrace "Cloud.Choice[T](Cloud<T option> computation)" workflow
    }

    /// <summary>
    ///     Returns a cloud computation that will execute the given computation on every available worker
    ///     possibly in parallel and will return when any of the supplied computations
    ///     have returned a successful value or if all of them fail to succeed. 
    ///     If a computation succeeds the rest of them are canceled.
    ///     The success of a computation is encoded as an option type.
    ///     This operator may create distribution.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<Cloud<'T option> * IWorkerRef>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let! receiver = runtime.ScheduleStartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
        return Cloud.WithAppendedStackTrace "Cloud.StartChild[T](Cloud<T> computation)" receiver
    }

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : Cloud<IWorkerRef> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : Cloud<IWorkerRef []> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.ProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud task.
    /// </summary>
    static member GetTaskId () : Cloud<string> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.TaskId
    }

    /// <summary>
    ///     Gets the current scheduling context.
    /// </summary>
    static member GetSchedulingContext () = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.SchedulingContext
    }

    /// <summary>
    ///     Sets a new scheduling context for target workflow.
    /// </summary>
    /// <param name="workflow">Target workflow.</param>
    /// <param name="schedulingContext">Target scheduling context.</param>
    [<CompilerMessage("'SetSchedulingContext' only intended for runtime implementers.", 444)>]
    static member WithSchedulingContext (schedulingContext : SchedulingContext) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider>()
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
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let runtime' = runtime.WithFaultPolicy policy
        return! Cloud.SetResource(workflow, runtime')
    }


[<AutoOpen>]
module CloudCombinators =
        
    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
        cloud { 
            let! result = 
                    Cloud.Parallel<obj> [| cloud { let! value = left in return value :> obj }; 
                                            cloud { let! value = right in return value :> obj } |]
            return (result.[0] :?> 'a, result.[1] :?> 'b) 
        }

    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel and returns the
    ///     result of the first computation that completes and cancels the other.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<|>) (left : Cloud<'a>) (right : Cloud<'a>) : Cloud<'a> =
        cloud {
            let! result = 
                Cloud.Choice [| cloud { let! value = left  in return Some (value) }
                                cloud { let! value = right in return Some (value) }  |]

            return result.Value
        }

    /// <summary>
    ///     Combines two cloud computations into one that executes them sequentially.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }