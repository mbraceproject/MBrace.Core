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
    static member FromContinuations(body : ExecutionContext -> Continuation<'T> -> unit) : Cloud<'T> = 
        Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else body ctx cont)

    /// <summary>
    ///     Starts a cloud workflow with given execution context in the current thread.
    /// </summary>
    /// <param name="cloudWorkflow">Cloud workflow to be executed.</param>
    /// <paran name"continuation">Root continuation for workflow.</param>
    /// <param name="context">Local execution context.</param>
    static member StartImmediate(cloudWorkflow : Cloud<'T>, continuation : Continuation<'T>, ?context : ExecutionContext) : unit =
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
                        Resources = match resources with None -> ResourceRegistry.Empty | Some r -> r
                        CancellationToken = ct
                    }

                let cont =
                    {
                        Success = fun _ t -> sc t
                        Exception = fun _ e -> ec e
                        Cancellation = fun _ c -> cc c
                    }

                do Trampoline.Reset()
                Cloud.StartImmediate(cloudWorkflow, cont, context))
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
        Cloud.FromContinuations(fun ctx cont ->
            let result =
                try 
                    let runtime = ctx.Resources.Resolve<IRuntimeProvider>()
                    let runtime' = runtime.WithSchedulingContext schedulingContext
                    Choice1Of2 runtime'
                with e -> Choice2Of2 e

            match result with
            | Choice2Of2 e -> cont.Exception ctx e
            | Choice1Of2 runtime' ->
                let ctx = { ctx with Resources = ctx.Resources.Register(runtime') }
                Cloud.StartImmediate(workflow, cont, ctx))


[<RequireQualifiedAccess>]
module Continuation =
    
    /// <summary>
    ///     Contravariant Continuation map combinator.
    /// </summary>
    /// <param name="f">Mapper function.</param>
    /// <param name="tcontext">Initial context.</param>
    let inline map (f : 'S -> 'T) (scont : Continuation<'T>) =
        {
            Success = fun ctx t -> scont.Success ctx (f t)
            Exception = scont.Exception
            Cancellation = scont.Cancellation
        }