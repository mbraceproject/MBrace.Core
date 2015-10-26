namespace MBrace.Core

open System
open System.Threading.Tasks

open MBrace.Core.Internals

#nowarn "444"

/// Cloud workflows user API
type Cloud =

    // region : core continuation API
    
    /// <summary>
    ///     Gets the current cancellation token.
    /// </summary>
    static member CancellationToken : LocalCloud<ICloudCancellationToken> = 
        mkLocal(fun ctx cont -> cont.Success ctx ctx.CancellationToken)

    /// Attempts to fetch a fault data record for the current computation.
    /// Returns 'Some' if the current computation is a retry of a previously faulted
    /// computation or 'None' if no fault has occurred.
    static member TryGetFaultData() : LocalCloud<FaultData option> = Cloud.TryGetResource<FaultData> ()

    /// Returns true if the current computation is a retry of a previously faulted computation.
    static member IsPreviouslyFaulted : LocalCloud<bool> = local {
        let! resources = Cloud.GetResourceRegistry()
        return resources.Contains<FaultData>()
    }

    /// <summary>
    ///     Raise an exception.
    /// </summary>
    /// <param name="e">exception to be raised.</param>
    static member Raise<'T> (e : exn) : LocalCloud<'T> = mkLocal (raiseM e)

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
    static member TryFinally(body : Cloud<'T>, finalizer : LocalCloud<unit>) : Cloud<'T> =
        mkCloud <| tryFinally body.Body finalizer.Body

    /// <summary>
    ///     Creates a cloud workflow that asynchronously sleeps for a given amount of time.
    /// </summary>
    /// <param name="millisecondsDue">Milliseconds to suspend computation.</param>
    static member Sleep(millisecondsDue : int) : LocalCloud<unit> = 
        Cloud.OfAsync<unit>(Async.Sleep millisecondsDue)

    /// <summary>
    ///     Wraps an asynchronous workflow into a cloud workflow.
    /// </summary>
    /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
    static member OfAsync<'T>(asyncWorkflow : Async<'T>) : LocalCloud<'T> = mkLocal(ofAsync asyncWorkflow)

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : #Cloud<'T>) : Cloud<unit> = cloud { let! _ = workflow in return () }

    /// <summary>
    ///     Disposes of a distributed resource.
    /// </summary>
    /// <param name="disposable">Resource to be disposed.</param>
    static member Dispose<'Disposable when 'Disposable :> ICloudDisposable>(disposable : 'Disposable) : LocalCloud<unit> =
        local { return! Cloud.OfAsync <| disposable.Dispose () }

    /// <summary>
    ///     Asynchronously awaits a System.Threading.Task for completion.
    /// </summary>
    /// <param name="task">Awaited task.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    static member AwaitTask(task : Task<'T>, ?timeoutMilliseconds : int) : LocalCloud<'T> =
        Local.FromContinuations(fun ctx cont ->
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
    static member Log(logEntry : string) : LocalCloud<unit> = local {
        let! runtime = Cloud.TryGetResource<IParallelismProvider> ()
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
    static member IsTargetedWorkerSupported : LocalCloud<bool> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider>()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        let! workers = Cloud.OfAsync <| runtime.GetAvailableWorkers()
        let workflow = runtime.ScheduleChoice (workers |> Seq.map (fun w -> computation, Some w))
        let stackTraceSig = sprintf "Cloud.ChoiceEverywhere(Cloud<%s option> computation)" typeof<'T>.Name
        return! Cloud.WithAppendedStackTrace stackTraceSig workflow
    }

    /// <summary>
    ///     Start cloud computation as a separate cloud process. Returns a cloud computation that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="cancellationToken">Specify cancellation token for the cloud process. Defaults to no cancellation token.</param>
    /// <param name="procName">Optional user-specified name for the cloud process.</param>
    static member CreateProcess(computation : Cloud<'T>, ?faultPolicy : FaultPolicy, ?target : IWorkerRef, ?cancellationToken:ICloudCancellationToken, ?procName:string) : Cloud<ICloudProcess<'T>> = cloud {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        let faultPolicy = defaultArg faultPolicy runtime.FaultPolicy
        return! runtime.ScheduleCreateProcess(computation, faultPolicy, ?cancellationToken = cancellationToken, ?target = target, ?procName = procName)
    }

    /// <summary>
    ///     Asynchronously waits for a cloud process to complete and returns its result.
    /// </summary>
    /// <param name="cloudProcess">Cloud process to be awaited.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite</param>
    static member AwaitProcess(cloudProcess : ICloudProcess<'T>, ?timeoutMilliseconds:int) : LocalCloud<'T> = local {
        return! Cloud.OfAsync <| cloudProcess.AwaitResultAsync(?timeoutMilliseconds = timeoutMilliseconds)
    }

    /// <summary>
    ///     Asynchronously waits until one of the given processes completes.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAny([<ParamArray>] processes : ICloudProcess []) : LocalCloud<ICloudProcess> =
        CloudProcessHelpers.WhenAny processes |> Cloud.OfAsync

    /// <summary>
    ///     Asynchronously waits until one of the given processes completes.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAny([<ParamArray>] processes : ICloudProcess<'T> []) : LocalCloud<ICloudProcess<'T>> =
        CloudProcessHelpers.WhenAny processes |> Cloud.OfAsync

    /// <summary>
    ///     Asynchronously waits until all of the given processes complete.
    /// </summary>
    /// <param name="processes">Input processes.</param>
    static member WhenAll([<ParamArray>] processes : ICloudProcess []) : LocalCloud<unit> =
        CloudProcessHelpers.WhenAll processes |> Cloud.OfAsync

    /// <summary>
    ///     Start cloud computation as child process. Returns a cloud computation that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    static member StartChild(workflow : Cloud<'T>, ?target : IWorkerRef) : Cloud<LocalCloud<'T>> = cloud {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        let! cancellationToken = Cloud.CancellationToken
        let! cloudProcess = runtime.ScheduleCreateProcess(workflow, runtime.FaultPolicy, cancellationToken, ?target = target)
        let stackTraceSig = sprintf "Cloud.StartChild(Cloud<%s> computation)" typeof<'T>.Name
        return Local.WithAppendedStackTrace stackTraceSig (local { return! Cloud.OfAsync <| cloudProcess.AwaitResultAsync() })
    }

    /// <summary>
    ///     Wraps provided cloud workflow as a local workflow.
    ///     Any distributed parallelism combinators called by the
    ///     workflow will be re-interpreted using thread parallelism semantics.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be wrapped.</param>
    static member AsLocal(workflow : Cloud<'T>) : LocalCloud<'T> = local {
        let isLocalEvaluated = ref false
        let updateCtx (ctx : ExecutionContext) =
            match ctx.Resources.TryResolve<IParallelismProvider> () with
            | None -> ctx
            | Some rp when rp.IsForcedLocalParallelismEnabled -> isLocalEvaluated := true ; ctx
            | Some rp -> { ctx with Resources = ctx.Resources.Register <| rp.WithForcedLocalParallelismSetting true }

        let revertCtx (ctx : ExecutionContext) =
            if !isLocalEvaluated then ctx
            else
                match ctx.Resources.TryResolve<IParallelismProvider> () with
                | None -> ctx
                | Some rp -> { ctx with Resources = ctx.Resources.Register <| rp.WithForcedLocalParallelismSetting false }

        let localEvaluated = Cloud.WithNestedContext(workflow, updateCtx, revertCtx)
        return! mkLocal localEvaluated.Body
    }

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : LocalCloud<IWorkerRef> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : LocalCloud<IWorkerRef []> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return! Cloud.OfAsync <| runtime.GetAvailableWorkers()
    }

    /// <summary>
    ///     Gets total number of available workers in cluster context.
    /// </summary>
    static member GetWorkerCount () : LocalCloud<int> = local {
        let! workers = Cloud.GetAvailableWorkers()
        return workers.Length
    }

    /// <summary>
    ///     Gets the assigned id of the currently running CloudProcess.
    /// </summary>
    static member GetCloudProcessId () : LocalCloud<string> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return runtime.CloudProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud work item.
    /// </summary>
    static member GetWorkItemId () : LocalCloud<string> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return runtime.WorkItemId
    }

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    static member FaultPolicy : LocalCloud<FaultPolicy> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        let currentPolicy = runtime.FaultPolicy
        let update (runtime : IParallelismProvider) = runtime.WithFaultPolicy policy
        let revert (runtime : IParallelismProvider) = runtime.WithFaultPolicy currentPolicy
        return! Cloud.WithNestedResource(workflow, update, revert)
    }

    /// Creates a new cloud cancellation token source
    static member CreateCancellationTokenSource () = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [||]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="parent">Parent cancellation token. Defaults to the current process cancellation token.</param>
    static member CreateLinkedCancellationTokenSource(?parent : ICloudCancellationToken) = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource [|token1 ; token2|]
    }

    /// <summary>
    ///     Creates a linked cloud cancellation token source.
    /// </summary>
    /// <param name="tokens">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        return! Cloud.OfAsync <| runtime.CreateLinkedCancellationTokenSource (Seq.toArray tokens)
    }


/// Local-parallelism combinators
type Local =

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Workflow to be protected.</param>
    static member Catch(workflow : LocalCloud<'T>) : LocalCloud<Choice<'T, exn>> = local {
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
    static member TryFinally(body : LocalCloud<'T>, finalizer : LocalCloud<unit>) : LocalCloud<'T> =
        mkLocal <| tryFinally body.Body finalizer.Body

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : LocalCloud<'T>) : LocalCloud<unit> = local { let! _ = workflow in return () }

    /// <summary>
    ///     Creates a cloud computation that will execute given computations to targeted workers
    ///     possibly in parallel and if successful returns the array of gathered results.
    ///     This operator may create distribution.
    ///     Exceptions raised by children carry cancellation semantics.
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<LocalCloud<'T>>) : LocalCloud<'T []> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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
    static member Parallel (left : LocalCloud<'L>, right : LocalCloud<'R>) : LocalCloud<'L * 'R> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
        let wrap (w : LocalCloud<_>) = local { let! r = w in return box r }
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
    static member Choice (computations : seq<LocalCloud<'T option>>) : LocalCloud<'T option> = local {
        let! runtime = Cloud.GetResource<IParallelismProvider> ()
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