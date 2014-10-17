namespace Nessos.MBrace

    #nowarn "444"

    open System.Threading
    open System.Threading.Tasks

    open Nessos.MBrace.Runtime

    /// Cloud workflows static methods
    type Cloud =
        
        /// <summary>
        ///     Creates a cloud workflow that captures the current execution context.
        /// </summary>
        /// <param name="body">Execution body.</param>
        [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
        static member FromContinuations(body : Context<'T> -> unit) : Cloud<'T> = 
            Body(fun ctx -> if ctx.IsCancellationRequested then ctx.Cancel() else body ctx)

        /// <summary>
        ///     Gets resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
        static member GetResource<'TResource> () : Cloud<'TResource> =
            Body(fun ctx ->
                if ctx.IsCancellationRequested then ctx.Cancel() else
                let res = protect (fun () -> ctx.Resource.Resolve<'TResource> ()) ()
                ctx.ChoiceCont res)

        /// <summary>
        ///     Try Getting resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
        static member TryGetResource<'TResource> () : Cloud<'TResource option> =
            Body(fun ctx -> 
                if ctx.IsCancellationRequested then ctx.Cancel() else
                ctx.scont <| ctx.Resource.TryResolve<'TResource> ())

        /// <summary>
        ///     Gets the current cancellation token.
        /// </summary>
        static member CancellationToken = Body(fun ctx -> ctx.scont ctx.CancellationToken)

        /// <summary>
        ///     Raise an exception.
        /// </summary>
        /// <param name="e">exception to be raised.</param>
        static member Raise<'T> (e : exn) : Cloud<'T> = raiseM e

        /// <summary>
        ///     Catch exception from given cloud workflow.
        /// </summary>
        /// <param name="cloudWorkflow"></param>
        static member Catch(cloudWorkflow : Cloud<'T>) = cloud {
            try
                let! res = cloudWorkflow
                return Choice1Of2 res
            with e ->
                return Choice2Of2 e
        }

        /// <summary>
        ///     Creates a cloud workflow that asynchronously sleeps for a given amount of time.
        /// </summary>
        /// <param name="timeoutMilliseconds"></param>
        static member Sleep(timeoutMilliseconds : int) : Cloud<unit> = Cloud.OfAsync<unit>(Async.Sleep timeoutMilliseconds)

        /// <summary>
        ///     Wraps an asynchronous workflow into a cloud workflow.
        /// </summary>
        /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
        static member OfAsync<'T>(asyncWorkflow : Async<'T>) : Cloud<'T> = 
            Body(fun ctx -> Async.StartWithContinuations(asyncWorkflow, ctx.scont, ctx.econt, ctx.ccont, ctx.CancellationToken))

        /// <summary>
        ///     Wraps a cloud workflow into an asynchronous workflow.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
        static member ToAsync(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry) : Async<'T> = async {
            let! ct = Async.CancellationToken
            return! 
                Async.FromContinuations(fun (sc,ec,cc) ->
                    let context = 
                        {
                            Resource = match resources with None -> ResourceRegistry.Empty | Some r -> r
                            CancellationToken = ct

                            scont = sc
                            econt = ec
                            ccont = cc
                        }

                    Cloud.StartImmediate(cloudWorkflow, context))
        }

        /// <summary>
        ///     Asynchronously await task completion
        /// </summary>
        /// <param name="task">Task to be awaited</param>
        static member AwaitTask (task : Task<'T>) = Cloud.OfAsync(Async.AwaitTask task)
            

        /// <summary>
        ///     Starts a cloud workflow with given execution context in the current thread.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="ctx">Local execution context.</param>
        static member StartImmediate(cloudWorkflow : Cloud<'T>, ctx : Context<'T>) = let (Body f) = cloudWorkflow in f ctx

        /// <summary>
        ///     Starts provided cloud workflow in the thread pool.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource registry passed to execution context.</param>
        /// <param name="cancellationToken">Local Cancellation token.</param>
        static member Start(cloudWorkflow : Cloud<unit>, ?resources, ?cancellationToken) =
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
        ///     Run a cloud workflow as a local computation.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
        /// <param name="cancellationToken">Cancellation token to be used.</param>
        static member RunSynchronously(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry, ?cancellationToken) : 'T =
            let wf = Cloud.ToAsync(cloudWorkflow, ?resources = resources) 
            Async.RunSynchronously(wf, ?cancellationToken = cancellationToken)

        /// <summary>
        ///     Cloud.Parallel combinator
        /// </summary>
        /// <param name="computations">Input computations to be executed in parallel.</param>
        static member Parallel (computations : seq<Cloud<'T>>) = cloud {
            let! runtime = Cloud.GetResource<IRuntimeProvider> ()
            return! runtime.ScheduleParallel computations
        }

        /// <summary>
        ///     Cloud.Choice combinator
        /// </summary>
        /// <param name="computations">Input computations to be executed in parallel.</param>
        static member Choice (computations : seq<Cloud<'T option>>) = cloud {
            let! runtime = Cloud.GetResource<IRuntimeProvider> ()
            return! runtime.ScheduleChoice computations
        }

        /// <summary>
        ///     Start cloud computation as child. Returns a cloud workflow that queries the result.
        /// </summary>
        /// <param name="computation">Computation to be executed.</param>
        /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds; defaults to infinite.</param>
        static member StartChild(computation : Cloud<'T>, ?target : IWorkerRef, ?timeoutMilliseconds:int) = cloud {
            let! runtime = Cloud.GetResource<IRuntimeProvider> ()
            return! runtime.ScheduleStartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
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
        static member SetSchedulingContext(workflow : Cloud<'T>, schedulingContext) : Cloud<'T> =
            Cloud.FromContinuations(fun ctx ->
                let result =
                    try 
                        let runtime = ctx.Resource.Resolve<IRuntimeProvider>()
                        let runtime' = runtime.WithSchedulingContext schedulingContext
                        Choice1Of2 runtime'
                    with e -> Choice2Of2 e

                match result with
                | Choice2Of2 e -> ctx.econt e
                | Choice1Of2 runtime' ->
                    let ctx = { ctx with Resource = ctx.Resource.Register(runtime') }
                    Cloud.StartImmediate(workflow, ctx))

        /// <summary>
        ///     Force thread local execution semantics for given cloud workflow.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        static member ToLocal(workflow : Cloud<'T>) = Cloud.SetSchedulingContext(workflow, ThreadParallel)

        /// <summary>
        ///     Force sequential execution semantics for given cloud workflow.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        static member ToSequential(workflow : Cloud<'T>) = Cloud.SetSchedulingContext(workflow, Sequential)


    /// Cloud reference methods
    type CloudRef =

        /// Allocate a new Cloud reference
        static member New(value : 'T) = cloud {
            let! storageProvider = Cloud.GetResource<IStorageProvider> ()
            return! storageProvider.CreateCloudRef value |> Cloud.OfAsync
        }

        /// Dereference a Cloud reference.
        static member Dereference(cloudRef : ICloudRef<'T>) = Cloud.OfAsync <| cloudRef.GetValue()