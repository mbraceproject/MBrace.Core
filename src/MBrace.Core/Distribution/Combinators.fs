namespace MBrace.Core

open System.Threading.Tasks

open MBrace.Core.Internals

#nowarn "444"

/// Cloud workflows user API
type Cloud =

    // region : core continuation API
    
    /// <summary>
    ///     Gets the current cancellation token.
    /// </summary>
    static member CancellationToken : Local<ICloudCancellationToken> = 
        mkLocal(fun ctx cont -> cont.Success ctx ctx.CancellationToken)

    /// <summary>
    ///     Raise an exception.
    /// </summary>
    /// <param name="e">exception to be raised.</param>
    static member Raise<'T> (e : exn) : Local<'T> = mkLocal (raiseM e)

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Workflow to be protected.</param>
    static member Catch(workflow : Cloud<'T>) : Cloud<Choice<'T, exn>> = cloud {
        try
            let! res = workflow
            return Choice1Of2 res
        with e ->
            return Choice2Of2 e
    }

    /// <summary>
    ///     Try/Finally combinator for monadic finalizers.
    /// </summary>
    /// <param name="body">Workflow body.</param>
    /// <param name="finalizer">Finalizer workflow.</param>
    static member TryFinally(body : Cloud<'T>, finalizer : Local<unit>) : Cloud<'T> =
        mkCloud <| tryFinally body.Body finalizer.Body

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
    static member OfAsync<'T>(asyncWorkflow : Async<'T>) : Local<'T> = local { return! asyncWorkflow }

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : #Cloud<'T>) : Cloud<unit> = cloud { let! _ = workflow in return () }

    /// <summary>
    ///     Disposes of a distributed resource.
    /// </summary>
    /// <param name="disposable">Resource to be disposed.</param>
    static member Dispose<'Disposable when 'Disposable :> ICloudDisposable>(disposable : 'Disposable) : Local<unit> =
        local { return! disposable.Dispose () }

    // region : runtime API

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Log(logEntry : string) : Local<unit> = local {
        let! runtime = Cloud.TryGetResource<IDistributionProvider> ()
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
        let! runtime = Cloud.GetResource<IDistributionProvider>()
        return runtime.IsTargetedWorkerSupported
    }

    /// <summary>
    ///     Creates a cloud computation that will execute the given computations
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<#Cloud<'T>>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun c -> c,None))
        let stackTraceSig = sprintf "Cloud.Parallel(seq<Cloud<%s>> computations)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute the given computations
    ///     possibly in parallel and if successful returns the pair of results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (left : Cloud<'L>, right : Cloud<'R>) : Cloud<'L * 'R> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let wrap (w : Cloud<_>) = (cloud { let! r = w in return box r } , None)
        let workflow = runtime.ScheduleParallel [| wrap left ; wrap right |]
        let stackTraceSig = sprintf "Cloud.Parallel(Cloud<%s> left, Cloud<%s> right)" typeof<'L>.Name typeof<'R>.Name
        let! result = Cloud.WithAppendedStackTrace stackTraceSig workflow
        return result.[0] :?> 'L, result.[1] :?> 'R
    }


    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<#Cloud<'T> * IWorkerRef>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleParallel (computations |> Seq.map (fun (c,w) -> c,Some w))
        let stackTraceSig = sprintf "Cloud.Parallel(seq<Cloud<%s> * IWorkerRef> computations)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute provided computation on every available worker
    ///     in the cluster and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Any exception raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computation">Computation to be executed in every worker.</param>
    static member ParallelEverywhere(computation : Cloud<'T>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleParallel (workers |> Seq.map (fun w -> computation, Some w))
        let stackTraceSig = sprintf "Cloud.ParallelEverywhere(Cloud<%s> computation)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
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
    static member Choice (computations : seq<#Cloud<'T option>>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun c -> c,None))
        let stackTraceSig = sprintf "Cloud.Choice(seq<Cloud<%s option>> computations)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
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
    static member Choice (computations : seq<#Cloud<'T option> * IWorkerRef>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleChoice (computations |> Seq.map (fun (c,w) -> c, Some w))
        let stackTraceSig = sprintf "Cloud.Choice(seq<Cloud<%s option> * IWorkerRef> computations)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
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
    static member ChoiceEverywhere (computation : Cloud<'T option>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleChoice (workers |> Seq.map (fun w -> computation, Some w))
        let stackTraceSig = sprintf "Cloud.ChoiceEverywhere(Cloud<%s option> computation)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
    }

    /// <summary>
    ///     Start cloud computation as a task. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="cancellationToken">Specify cancellation token for task. Defaults to no cancellation token.</param>
    /// <param name="taskName">Optional user-specified name for task.</param>
    static member StartAsTask(computation : Cloud<'T>, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?cancellationToken:ICloudCancellationToken, ?taskName:string) : Cloud<ICloudTask<'T>> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let faultPolicy = defaultArg faultPolicy runtime.FaultPolicy
        return! runtime.ScheduleStartAsTask(computation, faultPolicy, ?cancellationToken = cancellationToken, ?target = target, ?taskName = taskName)
    }

    /// <summary>
    ///     Asynchronously awaits cloud task for completion and returns its result.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite</param>
    static member AwaitTask(task : ICloudTask<'T>, ?timeoutMilliseconds:int) : Local<'T> = local {
        return! task.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
    }

    /// <summary>
    ///     Start cloud computation as child process. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    static member StartChild(workflow : Cloud<'T>, ?target : IWorkerRef) : Cloud<Local<'T>> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let! cancellationToken = Cloud.CancellationToken
        let! task = runtime.ScheduleStartAsTask(workflow, runtime.FaultPolicy, cancellationToken, ?target = target)
        let stackTraceSig = sprintf "Cloud.StartChild(Cloud<%s> computation)" typeof<'T>.Name
        return Local.WithAppendedStackTrace stackTraceSig (local { return! task.AwaitResult() })
    }

    /// <summary>
    ///     Wraps provided cloud workflow as a local workflow.
    ///     Any distributed parallelism combinators called by the
    ///     workflow will be re-interpreted using thread parallelism semantics.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be wrapped.</param>
    static member AsLocal(workflow : Cloud<'T>) : Local<'T> = local {
        let isLocalEvaluated = ref false
        let updateCtx (ctx : ExecutionContext) =
            match ctx.Resources.TryResolve<IDistributionProvider> () with
            | None -> ctx
            | Some rp when rp.IsForcedLocalParallelismEnabled -> isLocalEvaluated := true ; ctx
            | Some rp -> { ctx with Resources = ctx.Resources.Register <| rp.WithForcedLocalParallelismSetting true }

        let revertCtx (ctx : ExecutionContext) =
            if !isLocalEvaluated then ctx
            else
                match ctx.Resources.TryResolve<IDistributionProvider> () with
                | None -> ctx
                | Some rp -> { ctx with Resources = ctx.Resources.Register <| rp.WithForcedLocalParallelismSetting false }

        let localEvaluated = Cloud.WithNestedContext(workflow, updateCtx, revertCtx)
        return! mkLocal localEvaluated.Body
    }

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : Local<IWorkerRef> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : Local<IWorkerRef []> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
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
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return runtime.ProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud job.
    /// </summary>
    static member GetJobId () : Local<string> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return runtime.JobId
    }

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    static member FaultPolicy : Local<FaultPolicy> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let currentPolicy = runtime.FaultPolicy
        let update (runtime : IDistributionProvider) = runtime.WithFaultPolicy policy
        let revert (runtime : IDistributionProvider) = runtime.WithFaultPolicy currentPolicy
        return! Cloud.WithNestedResource(workflow, update, revert)
    }

    /// Creates a new cloud cancellation token source
    static member CreateCancellationTokenSource () = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [||]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="parent">Parent cancellation token. Defaults to the current process cancellation token.</param>
    static member CreateLinkedCancellationTokenSource(?parent : ICloudCancellationToken) = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
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
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [|token1 ; token2|]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="tokens">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource (Seq.toArray tokens)
    }


/// Local-parallelism combinators
type Local =

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Workflow to be protected.</param>
    static member Catch(workflow : Local<'T>) : Local<Choice<'T, exn>> = local {
        try
            let! res = workflow
            return Choice1Of2 res
        with e ->
            return Choice2Of2 e
    }

    /// <summary>
    ///     Try/Finally combinator for monadic finalizers.
    /// </summary>
    /// <param name="body">Workflow body.</param>
    /// <param name="finalizer">Finalizer workflow.</param>
    static member TryFinally(body : Local<'T>, finalizer : Local<unit>) : Local<'T> =
        mkLocal <| tryFinally body.Body finalizer.Body

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : Local<'T>) : Local<unit> = local { let! _ = workflow in return () }

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Local<'T>>) : Local<'T []> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleLocalParallel computations
        let stackTraceSig = sprintf "Local.Parallel(seq<Local<%s>> computations)" typeof<'T>.Name
        return! Local.WithAppendedStackTrace stackTraceSig workflow
    }

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (left : Local<'L>, right : Local<'R>) : Local<'L * 'R> = local {
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let wrap (w : Local<_>) = local { let! r = w in return box r }
        let workflow = runtime.ScheduleLocalParallel [| wrap left ; wrap right |]
        let stackTraceSig = sprintf "Local.Parallel(Local<%s> left, Local<%s> right)" typeof<'L>.Name typeof<'R>.Name
        let! result = Local.WithAppendedStackTrace stackTraceSig workflow
        return result.[0] :?> 'L, result.[1] :?> 'R
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
        let! runtime = Cloud.GetResource<IDistributionProvider> ()
        let workflow = runtime.ScheduleLocalChoice computations
        let stackTraceSig = sprintf "Local.Choice(seq<Local<%s option>> computations)" typeof<'T>.Name
        return! Local.WithAppendedStackTrace stackTraceSig workflow
    }



/// collection of parallelism operators for the cloud
[<AutoOpen>]
module CloudOperators =
        
    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let inline (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
        Cloud.Parallel(left, right)