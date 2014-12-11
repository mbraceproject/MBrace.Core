namespace Nessos.MBrace.Continuation

open System.Threading
open System.Threading.Tasks

open Nessos.MBrace
open Nessos.MBrace.Continuation

#nowarn "444"

/// Cloud workflows continuation API
type Cloud =
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : Cloud<'T> = 
        Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else body ctx cont)

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
    static member GetResourceRegistry () : Cloud<ResourceRegistry> =
        Cloud.FromContinuations(fun ctx cont -> cont.Success ctx ctx.Resources)

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
    static member ToAsync(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry) : Async<'T> = async {
        let! ct = Async.CancellationToken
        return! 
            Async.FromContinuations(fun (sc,ec,cc) ->
                let context = 
                    {
                        Resources = match resources with None -> ResourceRegistry.Empty | Some r -> r
                        CancellationToken = ct
                    }

                let cont =
                    {
                        Success = fun _ t -> sc t
                        Exception = fun _ edi -> ec (extract edi)
                        Cancellation = fun _ c -> cc c
                    }

                do Trampoline.Reset()
                Cloud.StartWithContinuations(cloudWorkflow, cont, context))
    }

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


namespace Nessos.MBrace

open System.Threading.Tasks
open Nessos.MBrace.Continuation

/// Cloud workflows user API
type Cloud =
    
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
        Cloud.OfAsync(disposable.Dispose())

    /// <summary>
    ///     Asynchronously await task completion
    /// </summary>
    /// <param name="task">Task to be awaited</param>
    static member AwaitTask (task : Task<'T>) : Cloud<'T> = 
        Cloud.OfAsync(Async.AwaitTask task)