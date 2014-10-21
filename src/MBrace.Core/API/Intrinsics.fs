namespace Nessos.MBrace.Runtime

open System.Threading
open System.Threading.Tasks

open Nessos.MBrace

#nowarn "444"

/// Intrinsic cloud workflow methods
type Cloud =
        
    /// <summary>
    ///     Creates a cloud workflow that captures the current execution context.
    /// </summary>
    /// <param name="body">Execution body.</param>
    [<CompilerMessage("'FromContinuations' only intended for runtime implementers.", 444)>]
    static member FromContinuations(body : Context<'T> -> unit) : Cloud<'T> = 
        Body(fun ctx -> if ctx.IsCancellationRequested then ctx.Cancel() else body ctx)

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="ctx">Local execution context.</param>
    static member StartImmediate(cloudWorkflow : Cloud<'T>, ctx : Context<'T>) : unit = 
        let (Body f) = cloudWorkflow in f ctx

    /// <summary>
    ///     Returns the resource registry for current execution context.
    /// </summary>
    [<CompilerMessage("'GetResourceRegistry' only intended for runtime implementers.", 444)>]
    static member GetResourceRegistry () : Cloud<ResourceRegistry> =
        Cloud.FromContinuations(fun ctx -> ctx.scont ctx.Resource)

    /// <summary>
    ///     Gets resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResource' only intended for runtime implementers.", 444)>]
    static member GetResource<'TResource> () : Cloud<'TResource> =
        Cloud.FromContinuations(fun ctx ->
            let res = protect (fun () -> ctx.Resource.Resolve<'TResource> ()) ()
            ctx.ChoiceCont res)

    /// <summary>
    ///     Try Getting resource from current execution context.
    /// </summary>
    [<CompilerMessage("'GetResources' only intended for runtime implementers.", 444)>]
    static member TryGetResource<'TResource> () : Cloud<'TResource option> =
        Cloud.FromContinuations(fun ctx -> ctx.scont <| ctx.Resource.TryResolve<'TResource> ())

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
    ///     Run a cloud workflow as a local computation.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <param name="resources">Resource resolver to be used; defaults to no resources.</param>
    /// <param name="cancellationToken">Cancellation token to be used.</param>
    static member RunSynchronously(cloudWorkflow : Cloud<'T>, ?resources : ResourceRegistry, ?cancellationToken) : 'T =
        let wf = Cloud.ToAsync(cloudWorkflow, ?resources = resources) 
        Async.RunSynchronously(wf, ?cancellationToken = cancellationToken)

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


[<RequireQualifiedAccess>]
module Context =
    
    /// <summary>
    ///     Contravariant map context map combinator.
    /// </summary>
    /// <param name="f">Mapper function.</param>
    /// <param name="tcontext">Initial context.</param>
    let inline map (f : 'S -> 'T) (tcontext : Context<'T>) =
        {
            Resource = tcontext.Resource
            CancellationToken = tcontext.CancellationToken
            scont = f >> tcontext.scont
            econt = tcontext.econt
            ccont = tcontext.ccont
        }