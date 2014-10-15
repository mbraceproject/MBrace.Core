namespace Nessos.MBrace

    open System.Threading

    /// Cloud workflows static methods
    type Cloud =
        
        /// <summary>
        ///     Creates a cloud workflow that captures the current execution context.
        /// </summary>
        /// <param name="body">Execution body.</param>
        [<CompilerMessage("'FromContinuations' only intended for runtime implementors.", 444)>]
        static member FromContinuations(body : Context<'T> -> unit) : Cloud<'T> = Body body

        /// <summary>
        ///     Gets resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementors.", 444)>]
        static member GetResource<'TResource> () : Cloud<'TResource> =
            Body(fun ctx ->
                let res = protect (fun () -> ctx.Resource.Resolve<'TResource> ()) ()
                ctx.ChoiceCont res)

        /// <summary>
        ///     Try Getting resource from current execution context.
        /// </summary>
        [<CompilerMessage("'GetResources' only intended for runtime implementors.", 444)>]
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
        ///     Starts a cloud workflow with given execution context.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="ctx">Local execution context.</param>
        static member StartWithContext(cloudWorkflow : Cloud<'T>, ctx : Context<'T>) = let (Body f) = cloudWorkflow in f ctx
        
        /// <summary>
        ///     Run a cloud workflow as a local asynchronous computation.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
        static member RunLocalAsync(cloudWorkflow : Cloud<'T>, ?resources : ResourceResolver) : Async<'T> = async {
            let! ct = Async.CancellationToken
            let tcs = new System.Threading.Tasks.TaskCompletionSource<'T>()
            let context = {
                Resource = 
                    match resources with
                    | None -> ResourceResolver.Empty
                    | Some r -> r

                CancellationToken = ct

                scont = tcs.SetResult
                econt = tcs.SetException
                ccont = fun _ -> tcs.SetCanceled ()
            }

            do Cloud.StartWithContext(cloudWorkflow, ctx = context)

            try return! Async.AwaitTask tcs.Task
            with :? System.AggregateException as e ->
                return raise e.InnerException
        }

        /// <summary>
        ///     Run a cloud workflow as a local computation.
        /// </summary>
        /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
        /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
        /// <param name="cancellationToken">Cancellation token to be used.</param>
        static member RunLocal(cloudWorkflow : Cloud<'T>, ?resources : ResourceResolver, ?cancellationToken) : 'T =
            let wf = Cloud.RunLocalAsync(cloudWorkflow, ?resources = resources) 
            Async.RunSynchronously(wf, ?cancellationToken = cancellationToken)


    type Async =
        
        static member StartWithCloudContext<'T> (ctx : Context<'T>) (workflow : Async<'T>) =
            Async.StartWithContinuations