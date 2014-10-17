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
        static member FromContinuations(body : Context<'T> -> unit) : Cloud<'T> = Body body

        /// <summary>
        ///     Gets resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
        static member GetResource<'TResource> () : Cloud<'TResource> =
            Body(fun ctx ->
                let res = protect (fun () -> ctx.Resource.Resolve<'TResource> ()) ()
                ctx.ChoiceCont res)

        /// <summary>
        ///     Try Getting resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
        static member TryGetResource<'TResource> () : Cloud<'TResource option> =
            Body(fun ctx -> ctx.scont <| ctx.Resource.TryResolve<'TResource> ())

        /// <summary>
        ///     Gets the current cancellation token.
        /// </summary>
        static member CancellationToken = Body(fun ctx -> ctx.scont ctx.CancellationToken)

        /// <summary>
        ///     Raise an exception.
        /// </summary>
        /// <param name="e">exception to be raised.</param>
        static member Raise<'T> (e : exn) = raise e

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
        ///     Wraps an asynchronous workflow into a cloud workflow.
        /// </summary>
        /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
        static member OfAsync(asyncWorkflow : Async<'T>) : Cloud<'T> = 
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
        ///     Starts given workflow as a separate, locally executing task.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource registry used with workflows.</param>
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
            let! scheduler = Cloud.GetResource<ISchedulingProvider> ()
            return! scheduler.Parallel computations
        }

        /// <summary>
        ///     Cloud.Choice combinator
        /// </summary>
        /// <param name="computations">Input computations to be executed in parallel.</param>
        static member Choice (computations : seq<Cloud<'T option>>) = cloud {
            let! scheduler = Cloud.GetResource<ISchedulingProvider> ()
            return! scheduler.Choice computations
        }

        /// <summary>
        ///     Start cloud computation as child. Returns a cloud workflow that queries the result.
        /// </summary>
        /// <param name="computation">Computation to be executed.</param>
        /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds; defaults to infinite.</param>
        static member StartChild(computation : Cloud<'T>, ?target : IWorkerRef, ?timeoutMilliseconds:int) = cloud {
            let! scheduler = Cloud.GetResource<ISchedulingProvider> ()
            return! scheduler.StartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
        }

        /// <summary>
        ///     Gets information on the execution cluster.
        /// </summary>
        static member GetRuntimeInfo () = cloud {
            let! infoProvider = Cloud.GetResource<ISchedulingProvider> ()
            return! Cloud.OfAsync <| infoProvider.GetRuntimeInfo()
        }

        /// <summary>
        ///     Gets number of workers on the cluster.
        /// </summary>
        static member GetWorkerCount () = cloud {
            let! info = Cloud.GetRuntimeInfo()
            return info.Workers.Length
        }

        /// <summary>
        ///     Gets the current scheduling context.
        /// </summary>
        static member GetSchedulingContext () = cloud {
            let! scheduler = Cloud.GetResource<ISchedulingProvider> ()
            return scheduler.Context
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
                        let scheduler = ctx.Resource.Resolve<ISchedulingProvider>()
                        let scheduler' = scheduler.WithContext schedulingContext
                        Choice1Of2 scheduler'
                    with e -> Choice2Of2 e

                match result with
                | Choice2Of2 e -> ctx.econt e
                | Choice1Of2 scheduler' ->
                    let ctx = { ctx with Resource = ctx.Resource.Register(scheduler') }
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