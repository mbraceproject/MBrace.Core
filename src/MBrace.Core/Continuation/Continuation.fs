namespace MBrace.Core.Internals

open System.Threading
open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Internal local workflow combinators
type Local =
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : CloudLocal<'T> = 
        mkLocal(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else body ctx cont)

    /// <summary>
    ///     Runs provided workflow in a nested execution context that is
    ///     introduced using the update/revert functions.
    ///     These must be serializable and exception safe.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="update">Resource updating function.</param>
    /// <param name="revert">Resource reverting function.</param>
    [<CompilerMessage("'WithNestedContext' only intended for runtime implementers.", 444)>]
    static member WithNestedContext(workflow : CloudLocal<'T>, 
                                        update : ExecutionContext -> ExecutionContext, 
                                        revert : ExecutionContext -> ExecutionContext) : CloudLocal<'T> =

        Local.FromContinuations(withNestedContext update revert workflow.Body)

    /// <summary>
    ///     Runs provided workflow in a nested execution context that is
    ///     introduced using the update/revert functions.
    ///     These must be serializable and exception safe.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="update">Resource updating function.</param>
    /// <param name="revert">Resource reverting function.</param>
    [<CompilerMessage("'WithNestedResource' only intended for runtime implementers.", 444)>]
    static member WithNestedResource(workflow : Cloud<'T>, 
                                        update : 'TResource -> 'TResource, 
                                        revert : 'TResource -> 'TResource) : CloudLocal<'T> =

        let updateCtx f (ctx : ExecutionContext) =
            let tres = ctx.Resources.Resolve<'TResource> ()
            { ctx with Resources = ctx.Resources.Register(f tres) }

        Local.FromContinuations(withNestedContext (updateCtx update) (updateCtx revert) workflow.Body) 

    /// <summary>
    ///     Wraps a workflow with a mapped continuation.
    /// </summary>
    /// <param name="mapper">Continuation mapping function.</param>
    /// <param name="workflow">Input workflow.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithMappedContinuation (mapper : Continuation<'T> -> Continuation<'S>) (workflow : CloudLocal<'S>) : CloudLocal<'T> =
        mkLocal (fun ctx cont -> workflow.Body ctx (mapper cont))

    /// <summary>
    ///     Appends a function information entry to the symbolic stacktrace in the exception continuation.
    /// </summary>
    /// <param name="functionName">Function info string to be appended.</param>
    /// <param name="workflow">Workflow to be wrapped.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithAppendedStackTrace (functionName : string) (workflow : CloudLocal<'T>) : CloudLocal<'T> =
        mkLocal (fun ctx cont ->
            let cont' = { cont with Exception = fun ctx edi -> cont.Exception ctx (appendToStacktrace functionName edi) }
            workflow.Body ctx cont')

/// Internal cloud workflow combinators
type Cloud private () =

    static let getCancellationToken (ct : ICloudCancellationToken option) =
        match ct with
        | Some ct -> ct
        | None -> new InMemoryCancellationToken() :> _
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : Cloud<'T> = 
        mkCloud(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else body ctx cont)

    /// <summary>
    ///     Runs provided workflow in a nested execution context that is
    ///     introduced using the update/revert functions.
    ///     These must be serializable and exception safe.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="update">Resource updating function.</param>
    /// <param name="revert">Resource reverting function.</param>
    [<CompilerMessage("'WithNestedContext' only intended for runtime implementers.", 444)>]
    static member WithNestedContext(workflow : Cloud<'T>, 
                                        update : ExecutionContext -> ExecutionContext, 
                                        revert : ExecutionContext -> ExecutionContext) : Cloud<'T> =

        Cloud.FromContinuations(withNestedContext update revert workflow.Body) 

    /// <summary>
    ///     Runs provided workflow in a nested execution context that is
    ///     introduced using the update/revert functions.
    ///     These must be serializable and exception safe.
    /// </summary>
    /// <param name="workflow">Workflow to be wrapped.</param>
    /// <param name="update">Resource updating function.</param>
    /// <param name="revert">Resource reverting function.</param>
    [<CompilerMessage("'WithNestedResource' only intended for runtime implementers.", 444)>]
    static member WithNestedResource(workflow : Cloud<'T>, 
                                        update : 'TResource -> 'TResource, 
                                        revert : 'TResource -> 'TResource) : Cloud<'T> =

        let updateCtx f (ctx : ExecutionContext) =
            let tres = ctx.Resources.Resolve<'TResource> ()
            { ctx with Resources = ctx.Resources.Register(f tres) }

        Cloud.FromContinuations(withNestedContext (updateCtx update) (updateCtx revert) workflow.Body) 

    /// <summary>
    ///     Wraps a workflow with a mapped continuation.
    /// </summary>
    /// <param name="mapper">Continuation mapping function.</param>
    /// <param name="workflow">Input workflow.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithMappedContinuation (mapper : Continuation<'T> -> Continuation<'S>) (workflow : #Cloud<'S>) : Cloud<'T> =
        mkCloud (fun ctx cont -> workflow.Body ctx (mapper cont))

    /// <summary>
    ///     Appends a function information entry to the symbolic stacktrace in the exception continuation.
    /// </summary>
    /// <param name="functionName">Function info string to be appended.</param>
    /// <param name="workflow">Workflow to be wrapped.</param>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member WithAppendedStackTrace (functionName : string) (workflow : #Cloud<'T>) : Cloud<'T> =
        mkCloud (fun ctx cont ->
            let cont' = { cont with Exception = fun ctx edi -> cont.Exception ctx (appendToStacktrace functionName edi) }
            workflow.Body ctx cont')


    /// <summary>
    ///     Returns the execution context of current computation.
    /// </summary>
    [<CompilerMessage("'GetExecutionContext' only intended for runtime implementers.", 444)>]
    static member GetExecutionContext () : CloudLocal<ExecutionContext> =
        Local.FromContinuations(fun ctx cont -> cont.Success ctx ctx)
        
    /// <summary>
    ///     Returns the resource registry for current execution context.
    /// </summary>
    [<CompilerMessage("'GetResourceRegistry' only intended for runtime implementers.", 444)>]
    static member GetResourceRegistry () : CloudLocal<ResourceRegistry> =
        Local.FromContinuations(fun ctx cont -> cont.Success ctx ctx.Resources)

    /// <summary>
    ///     Gets resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResource' only intended for runtime implementers.", 444)>]
    static member GetResource<'TResource> () : CloudLocal<'TResource> =
        Local.FromContinuations(fun ctx cont ->
            let res = ValueOrException.protect (fun () -> ctx.Resources.Resolve<'TResource> ()) ()
            cont.ContinueWith(ctx, res))

    /// <summary>
    ///     Try Getting resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member TryGetResource<'TResource> () : CloudLocal<'TResource option> =
        Local.FromContinuations(fun ctx cont -> cont.Success ctx <| ctx.Resources.TryResolve<'TResource> ())

    /// <summary>
    ///     Starts given workflow as a System.Threading.Task
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry used with workflows.</param>
    /// <param name="taskCreationOptions">Resource registry used with workflows.</param>
    /// <param name="cancellationToken">Cancellation token used for task. Defaults to no cancellation token.</param>
    [<CompilerMessage("'StartAsTask' only intended for runtime implementers.", 444)>]
    static member StartAsTask(workflow : Cloud<'T>, resources : ResourceRegistry, 
                                ?cancellationToken : ICloudCancellationToken, ?taskCreationOptions : TaskCreationOptions) : Task<'T> =

        let taskCreationOptions = defaultArg taskCreationOptions TaskCreationOptions.None
        let tcs = new TaskCompletionSource<'T>(taskCreationOptions)
        let cancellationToken = getCancellationToken cancellationToken

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
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to computation. Defaults to the empty resource registry.</param>
    /// <param name="cancellationToken">Specifies a cancellation token for the cloud workflow. Defaults to inherited Async cancellation token.</param>
    [<CompilerMessage("'ToAsync' only intended for runtime implementers.", 444)>]
    static member ToAsync(workflow : Cloud<'T>, ?resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) : Async<'T> = async {
        let resources = defaultArg resources ResourceRegistry.Empty
        let! cancellationToken = async {
            match cancellationToken with
            | Some ct -> return ct
            | None ->
                let! ct = Async.CancellationToken
                return new InMemoryCancellationToken(ct) :> ICloudCancellationToken
        }
        
        return! Async.FromContinuations(fun (sc,ec,cc) ->
            let cont =
                {
                    Success = fun _ t -> sc t
                    Exception = fun _ edi -> ec (extract edi)
                    Cancellation = fun _ c -> cc c
                }

            do Trampoline.Reset()
            Cloud.StartWithContinuations(workflow, cont, resources, cancellationToken))
    }

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="continuation">Root continuation for workflow.</param>
    /// <param name="context">Local execution context.</param>
    [<CompilerMessage("'StartWithContinuations' only intended for runtime implementers.", 444)>]
    static member StartWithContinuations(workflow : Cloud<'T>, continuation : Continuation<'T>, ?context : ExecutionContext) : unit =
        let context = match context with Some c -> c | None -> ExecutionContext.Empty()
        workflow.Body context continuation

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="continuation">Root continuation for workflow.</param>
    /// <param name="resources">Resource registry for workflow. Defaults to the empty resource registry.</param>
    /// <param name="cancellationToken">Cancellation token used for workflow. Defaults to no cancellation token.</param>
    [<CompilerMessage("'StartWithContinuations' only intended for runtime implementers.", 444)>]
    static member StartWithContinuations(workflow : Cloud<'T>, continuation : Continuation<'T>, 
                                            ?resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) : unit =

        let resources = defaultArg resources ResourceRegistry.Empty
        let cancellationToken = getCancellationToken cancellationToken
        workflow.Body { Resources = resources ; CancellationToken = cancellationToken } continuation

    /// <summary>
    ///     Starts provided cloud workflow immediately in the current thread.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context. Defaults to the empty resource registry.</param>
    /// <param name="cancellationToken">Cancellation token used for computation. Defaults to no cancellation token.</param>
    [<CompilerMessage("'StartImmediate' only intended for runtime implementers.", 444)>]
    static member StartImmediate(workflow : Cloud<unit>, ?resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) : unit =
        let resources = defaultArg resources ResourceRegistry.Empty
        let cancellationToken = getCancellationToken cancellationToken
        let cont =
            {
                Success = fun _ _ -> ()
                Exception = fun _ edi -> ExceptionDispatchInfo.raise true edi
                Cancellation = fun _ _ -> ()
            }
            
        Cloud.StartWithContinuations(workflow, cont, resources, cancellationToken)

    /// <summary>
    ///     Starts provided cloud workflow in the thread pool.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context. Defaults to the empty resource registry.</param>
    /// <param name="cancellationToken">Cancellation token used for computation. Defaults to no cancellation token.</param>
    [<CompilerMessage("'Start' only intended for runtime implementers.", 444)>]
    static member Start(workflow : Cloud<unit>, ?resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) : unit =
        let resources = defaultArg resources ResourceRegistry.Empty
        let cancellationToken = getCancellationToken cancellationToken
        Trampoline.QueueWorkItem(fun () -> Cloud.StartImmediate(workflow, resources, cancellationToken))

    /// <summary>
    ///     Synchronously await a locally executing workflow.
    /// </summary>
    /// <param name="workflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource registry passed to execution context. Defaults to the empty resource registry.</param>
    /// <param name="cancellationToken">Cancellation token used for computation. Defaults to no cancellation token.</param>
    [<CompilerMessage("'RunSynchronously' only intended for runtime implementers.", 444)>]
    static member RunSynchronously(workflow : Cloud<'T>, ?resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) : 'T =
        let resources = defaultArg resources ResourceRegistry.Empty
        let cancellationToken = getCancellationToken cancellationToken
        Cloud.StartAsTask(workflow, resources, cancellationToken).CorrectResult