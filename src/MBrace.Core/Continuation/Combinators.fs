namespace MBrace.Continuation

open System.Threading
open System.Threading.Tasks

open MBrace
open MBrace.Continuation

#nowarn "444"

/// Cloud workflows core continuation API
type Cloud =
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : Workflow<_,'T> = 
        Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else body ctx cont)

    /// <summary>
    ///     Returns the execution context of current computation.
    /// </summary>
    [<CompilerMessage("'GetExecutionContext' only intended for runtime implementers.", 444)>]
    static member GetExecutionContext () : Local<ExecutionContext> =
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx)
        
    /// <summary>
    ///     Returns the resource registry for current execution context.
    /// </summary>
    [<CompilerMessage("'GetResourceRegistry' only intended for runtime implementers.", 444)>]
    static member GetResourceRegistry () : Local<ResourceRegistry> =
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx.Resources)

    /// <summary>
    ///     Gets resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResource' only intended for runtime implementers.", 444)>]
    static member GetResource<'TResource> () : Local<'TResource> =
        Cloud.FromContinuations(fun ctx cont ->
            let res = protect (fun () -> ctx.Resources.Resolve<'TResource> ()) ()
            cont.Choice(ctx, res))

    /// <summary>
    ///     Try Getting resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member TryGetResource<'TResource> () : Local<'TResource option> =
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx <| ctx.Resources.TryResolve<'TResource> ())

    /// <summary>
    ///     Installs provided resource to the scoped computation.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="resource">Resource to be installed.</param>
    [<CompilerMessage("'WithResource' only intended for runtime implementers.", 444)>]
    static member WithResource(workflow : Workflow<'C, 'T>, resource : 'Resource) : Workflow<'C, 'T> =
        Cloud.FromContinuations(fun ctx cont ->
            let (Body f) = workflow
            // Augment the continuation with undo logic so that resource
            // updates only hold within scope.
            let parentResource = ctx.Resources.TryResolve<'Resource>()
            let inline revertCtx (ctx : ExecutionContext) =
                let resources =
                    match parentResource with
                    | None -> ctx.Resources.Remove<'Resource> ()
                    | Some pr -> ctx.Resources.Register<'Resource> pr

                { ctx with Resources = resources }

            let cont' = 
                { 
                    Success = fun ctx t -> cont.Success (revertCtx ctx) t
                    Exception = fun ctx e -> cont.Exception (revertCtx ctx) e
                    Cancellation = fun ctx c -> cont.Cancellation (revertCtx ctx) c
                }

            f { ctx with Resources = ctx.Resources.Register resource } cont')

    /// <summary>
    ///     Wraps a workflow with a mapped continuation.
    /// </summary>
    /// <param name="mapper">Continuation mapping function.</param>
    /// <param name="workflow">Input workflow.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithMappedContinuation (mapper : Continuation<'T> -> Continuation<'S>) (workflow : Workflow<'C, 'S>) : Workflow<'C, 'T> =
        Body(fun ctx cont -> let (Body f) = workflow in f ctx (mapper cont))

    /// <summary>
    ///     Appends a function information entry to the symbolic stacktrace in the exception continuation.
    /// </summary>
    /// <param name="functionName">Function info string to be appended.</param>
    /// <param name="workflow">Workflow to be wrapped.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithAppendedStackTrace (functionName : string) (workflow : Workflow<'C, 'T>) : Workflow<'C, 'T> =
        Body(fun ctx cont ->
            let cont' = { cont with Exception = fun ctx edi -> cont.Exception ctx (appendToStacktrace functionName edi) }
            let (Body f) = workflow
            f ctx cont')

    /// <summary>
    ///     Starts given workflow as a System.Threading.Task
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry used with workflows.</param>
    /// <param name="taskCreationOptions">Resource registry used with workflows.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [<CompilerMessage("'StartAsTask' only intended for runtime implementers.", 444)>]
    static member StartAsTask(workflow : Workflow<'T>, resources : ResourceRegistry, 
                                cancellationToken : ICloudCancellationToken, ?taskCreationOptions : TaskCreationOptions) : Task<'T> =

        let taskCreationOptions = defaultArg taskCreationOptions TaskCreationOptions.None
        let tcs = new TaskCompletionSource<'T>(taskCreationOptions)
        let cont = 
            {
                Success = fun _ t -> tcs.TrySetResult t |> ignore
                Exception = fun _ edi -> tcs.TrySetException (extract edi) |> ignore
                Cancellation = fun _ _ -> tcs.TrySetCanceled() |> ignore
            }

        Trampoline.QueueWorkItem(fun () -> Cloud.StartWithContinuations(workflow, cont, resources, cancellationToken))
        tcs.Task

    /// <summary>
    ///     Wraps a cloud workflow into an asynchronous workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource resolver to be used; defaults to empty resource registry.</param>
    [<CompilerMessage("'ToAsync' only intended for runtime implementers.", 444)>]
    static member ToAsync(workflow : Workflow<'T>, resources : ResourceRegistry, cancellationToken : ICloudCancellationToken) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,cc) ->
            let cont =
                {
                    Success = fun _ t -> sc t
                    Exception = fun _ edi -> ec (extract edi)
                    Cancellation = fun _ c -> cc c
                }

            do Trampoline.Reset()
            Cloud.StartWithContinuations(workflow, cont, resources, cancellationToken))

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <paran name"continuation">Root continuation for workflow.</param>
    /// <param name="context">Local execution context.</param>
    [<CompilerMessage("'StartWithContinuations' only intended for runtime implementers.", 444)>]
    static member StartWithContinuations(workflow : Workflow<'T>, continuation : Continuation<'T>, context : ExecutionContext) : unit =
        let (Body f) = workflow in f context continuation

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="continuation">Root continuation for workflow.</param>
    /// <param name="resources">Resource registry for workflow.</param>
    /// <param name="cancellationToken">Cancellation token for workflow.</param>
    [<CompilerMessage("'StartWithContinuations' only intended for runtime implementers.", 444)>]
    static member StartWithContinuations(workflow : Workflow<'T>, continuation : Continuation<'T>, 
                                            resources : ResourceRegistry, cancellationToken : ICloudCancellationToken) : unit =
        let (Body f) = workflow
        f { Resources = resources ; CancellationToken = cancellationToken } continuation

    /// <summary>
    ///     Starts provided cloud workflow immediately in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context.</param>
    /// <param name="cancellationToken">Local Cancellation token.</param>
    [<CompilerMessage("'StartImmediate' only intended for runtime implementers.", 444)>]
    static member StartImmediate(cloudWorkflow : Workflow<unit>, resources : ResourceRegistry, cancellationToken : ICloudCancellationToken) : unit =
        let cont =
            {
                Success = fun _ _ -> ()
                Exception = fun _ edi -> ExceptionDispatchInfo.raise true edi
                Cancellation = fun _ _ -> ()
            }
            
        Cloud.StartWithContinuations(cloudWorkflow, cont, resources, cancellationToken)

    /// <summary>
    ///     Starts provided cloud workflow in the thread pool.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context.</param>
    /// <param name="cancellationToken">Local Cancellation token.</param>
    [<CompilerMessage("'Start' only intended for runtime implementers.", 444)>]
    static member Start(cloudWorkflow : Workflow<unit>, resources : ResourceRegistry, cancellationToken : ICloudCancellationToken) : unit =
        Trampoline.QueueWorkItem(fun () -> Cloud.StartImmediate(cloudWorkflow, resources, cancellationToken))

    /// <summary>
    ///     Synchronously await a locally executing workflow.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
    /// <param name="cancellationToken">Cancellation token to be used.</param>
    [<CompilerMessage("'RunSynchronously' only intended for runtime implementers.", 444)>]
    static member RunSynchronously(cloudWorkflow : Workflow<'T>, resources : ResourceRegistry, cancellationToken : ICloudCancellationToken) : 'T =
        Cloud.StartAsTask(cloudWorkflow, resources, cancellationToken).GetResult()