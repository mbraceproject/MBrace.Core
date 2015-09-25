module internal MBrace.Runtime.Combinators

open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

#nowarn "444"

let inline private withCancellationToken (cts : ICloudCancellationToken) (ctx : ExecutionContext) =
    { ctx with CancellationToken = cts }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> WorkItemExecutionMonitor.ProtectAsync ctx (f ctx cont))

let private ensureSerializable (t : 'T) =
    try FsPickler.EnsureSerializable t ; None
    with e -> Some e

let private extractWorkerId (runtime : IRuntimeManager) (worker : IWorkerRef option) =
    match worker with
    | None -> None
    | Some(:? WorkerRef as w) when areReflectiveEqual w.RuntimeId runtime.Id  -> Some w.WorkerId
    | _ -> invalidArg "target" <| sprintf "WorkerRef '%O' does not belong to the cluster." worker

let private extractWorkerIds (runtime : IRuntimeManager) (computations : (#Cloud<'T> * IWorkerRef option) []) =
    computations
    |> Array.map (fun (c,w) -> c, extractWorkerId runtime w)

/// <summary>
///     Defines a workflow that schedules provided cloud workflows for parallel computation.
/// </summary>
/// <param name="runtime">Runtime management object.</param>
/// <param name="parentProc">Parent cloud process info object.</param>
/// <param name="faultPolicy">Current cloud work item being executed.</param>
/// <param name="computations">Computations to be executed in parallel.</param>
let runParallel (runtime : IRuntimeManager) (parentProc : ICloudProcessEntry) 
                (faultPolicy : IFaultPolicy) (computations : seq<#Cloud<'T> * IWorkerRef option>) : Cloud<'T []> =

    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        // Early detect if return type is not serializable.
        | Choice1Of2 _ when not <| FsPickler.IsSerializableType<'T>() ->
            let msg = sprintf "Cloud.Parallel workflow uses non-serializable type '%s'." Type.prettyPrint<'T>
            let e = new SerializationException(msg)
            cont.Exception ctx (ExceptionDispatchInfo.Capture e)

        | Choice1Of2 [| |] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current work item
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] ->
            let clone = try FsPickler.Clone ((comp, cont)) |> Choice1Of2 with e -> Choice2Of2 e
            match clone with
            | Choice1Of2 (comp, cont) -> 
                let cont' = Continuation.map (fun t -> [| t |]) cont
                Cloud.StartWithContinuations(comp, cont', ctx)

            | Choice2Of2 e ->
                let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                let se = new SerializationException(msg, e)
                cont.Exception ctx (ExceptionDispatchInfo.Capture se)


        | Choice1Of2 computations ->
            // distributed computation, ensure that closures are serializable
            match ensureSerializable (computations, cont) with
            | Some e ->
                let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                let se = new SerializationException(msg, e)
                cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | None ->

            // ensure that target workers are valid in the current cluster context
            let computations = extractWorkerIds runtime computations

            let N = computations.Length
            let parallelWorkflowId = mkUUID()
            runtime.SystemLogger.Logf LogLevel.Info "Starting Cloud.Parallel<%s> workflow %s of %d children." Type.prettyPrint<'T> parallelWorkflowId N

            // request runtime resources required for distribution coordination
            let currentCts = ctx.CancellationToken
            let! childCts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, [|currentCts|], elevate = true)
            let! resultAggregator = runtime.ResultAggregatorFactory.CreateResultAggregator<'T>(aggregatorId = parallelWorkflowId, capacity = N)
            let! cancellationLatch = runtime.CounterFactory.CreateCounter(initialValue = 0)

            let onSuccess i ctx (t : 'T) = 
                async {
                    let logger = ctx.Resources.Resolve<IRuntimeManager>().SystemLogger
                    // check if result value can be serialized first.
                    match ensureSerializable t with
                    | Some e ->
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                            logger.Logf LogLevel.Debug "Cloud.Parallel<%s> workflow %s child #%d failed to serialize result." Type.prettyPrint<'T> parallelWorkflowId i
                            childCts.Cancel()
                            let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." Type.prettyPrint<'T> 
                            let se = new SerializationException(msg, e)
                            cont.Exception (withCancellationToken currentCts ctx) (ExceptionDispatchInfo.Capture se)

                        else // cancellation already triggered by different party, just declare work item completed.
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                    | None ->
                        logger.Logf LogLevel.Debug "Cloud.Parallel<%s> workflow %s child #%d completed." Type.prettyPrint<'T> parallelWorkflowId i
                        let workerId = ctx.Resources.Resolve<IWorkerId> ()
                        let! isCompleted = resultAggregator.SetResult(i, t, workerId)
                        if isCompleted then
                            logger.Logf LogLevel.Debug "Cloud.Parallel<%s> workflow %s has completed successfully." Type.prettyPrint<'T> parallelWorkflowId
                            // this is the last child callback, aggregate result and call parent continuation
                            let! results = resultAggregator.ToArray()
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) results

                        else // results pending, just declare work item completed.
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            let onException i ctx e =
                async {
                    let logger = ctx.Resources.Resolve<IRuntimeManager>().SystemLogger
                    match ensureSerializable e with
                    | Some e ->
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                            logger.Logf LogLevel.Debug "Cloud.Parallel<%s> workflow %s child #%d failed to serialize result." Type.prettyPrint<'T> parallelWorkflowId i
                            childCts.Cancel()
                            let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." Type.prettyPrint<'T> 
                            let se = new SerializationException(msg, e)
                            cont.Exception (withCancellationToken currentCts ctx) (ExceptionDispatchInfo.Capture se)

                        else // cancellation already triggered by different party, just declare work item completed.
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                        
                    | None ->
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                            logger.Logf LogLevel.Debug "Cloud.Parallel<%s> workflow %s child #%d has raised an exception." Type.prettyPrint<'T> parallelWorkflowId i
                            childCts.Cancel()
                            cont.Exception (withCancellationToken currentCts ctx) e
                        else // cancellation already triggered by different party, declare work item completed.
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else // cancellation already triggered by different party, declare work item completed.
                        WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            // Create wok items and enqueue
            do!
                computations
                |> Array.mapi (fun i (c,w) -> CloudWorkItem.Create(parentProc, childCts, faultPolicy, onSuccess i, onException i, onCancellation, CloudWorkItemType.ParallelChild(i, computations.Length), c, ?target = w))
                |> runtime.WorkItemQueue.BatchEnqueue
                    
            WorkItemExecutionMonitor.TriggerCompletion ctx })

/// <summary>
///     Defines a workflow that schedules provided nondeterministic cloud workflows for parallel computation.
/// </summary>
/// <param name="runtime">Runtime management object.</param>
/// <param name="parentProc">Parent cloud process info object.</param>
/// <param name="faultPolicy">Current cloud work item being executed.</param>
/// <param name="computations">Computations to be executed in parallel.</param>
let runChoice (runtime : IRuntimeManager) (parentProc : ICloudProcessEntry) 
                (faultPolicy : IFaultPolicy) (computations : seq<#Cloud<'T option> * IWorkerRef option>) =

    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current work item
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] -> 
            let clone = try FsPickler.Clone ((comp, cont)) |> Choice1Of2 with e -> Choice2Of2 e
            match clone with
            | Choice1Of2 (comp, cont) -> Cloud.StartWithContinuations(comp, cont, ctx)
            | Choice2Of2 e ->
                let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                let se = new SerializationException(msg, e)
                cont.Exception ctx (ExceptionDispatchInfo.Capture se)

        | Choice1Of2 computations ->
            // distributed computation, ensure that closures are serializable
            match ensureSerializable (computations, cont) with
            | Some e ->
                let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." Type.prettyPrint<'T>
                let se = new SerializationException(msg, e)
                cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | None ->

            // ensure that target workers are valid in the current cluster context
            let computations = extractWorkerIds runtime computations

            let N = computations.Length // avoid capturing computation array in continuation closures
            let choiceWorkflowId = mkUUID()
            runtime.SystemLogger.Logf LogLevel.Info "Starting Cloud.Choice<%s> workflow '%s' of %d children." Type.prettyPrint<'T> choiceWorkflowId N

            // request runtime resources required for distribution coordination
            let currentCts = ctx.CancellationToken
            let! childCts = CloudCancellationToken.Create(runtime.CancellationEntryFactory, [|currentCts|], elevate = true)
            let! completionLatch = runtime.CounterFactory.CreateCounter(initialValue = 0)
            let! cancellationLatch = runtime.CounterFactory.CreateCounter(initialValue = 0)

            let onSuccess i ctx (topt : 'T option) =
                async {
                    let logger = ctx.Resources.Resolve<IRuntimeManager>().SystemLogger
                    if Option.isSome topt then // 'Some' result, attempt to complete workflow
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then 
                            logger.Logf LogLevel.Debug "Cloud.Choice<%s> workflow %s child #%d has completed with result." Type.prettyPrint<'T> choiceWorkflowId i
                            // first child to initiate cancellation, grant access to parent scont
                            childCts.Cancel ()
                            cont.Success (withCancellationToken currentCts ctx) topt
                        else
                            // workflow already cancelled, declare work item completion
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                    else
                        logger.Logf LogLevel.Debug "Cloud.Choice<%s> workflow %s child #%d has completed without result." Type.prettyPrint<'T> choiceWorkflowId i
                        // 'None', increment completion latch
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = N then 
                            logger.Logf LogLevel.Debug "Cloud.Choice<%s> workflow %s has completed without result." Type.prettyPrint<'T> choiceWorkflowId
                            // is last work item to complete with 'None', pass None to parent scont
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            // other work items pending, declare work item completion
                            WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            let onException i ctx e =
                async {
                    let logger = ctx.Resources.Resolve<IRuntimeManager>().SystemLogger
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                        logger.Logf LogLevel.Debug "Cloud.Choice<%s> workflow %s child #%d raised an exception." Type.prettyPrint<'T> choiceWorkflowId i
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare work item completed.
                        WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first work item to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else // cancellation already triggered by different party, declare work item completed.
                        WorkItemExecutionMonitor.TriggerCompletion ctx
                } |> WorkItemExecutionMonitor.ProtectAsync ctx

            // create child work items
            do!
                computations
                |> Array.mapi (fun i (c,w) -> CloudWorkItem.Create(parentProc, childCts, faultPolicy, onSuccess i, onException i, onCancellation, CloudWorkItemType.ChoiceChild(i, computations.Length), c, ?target = w))
                |> runtime.WorkItemQueue.BatchEnqueue
                    
            WorkItemExecutionMonitor.TriggerCompletion ctx })

/// <summary>
///     Executes provided cloud workflow as a cloud process using the provided resources and parameters.
/// </summary>
/// <param name="runtime">Runtime management object.</param>
/// <param name="parentProc">Identifies whether cloud process is being enqueued as part of a parent cloud process or of a client-side enqueue.</param>
/// <param name="dependencies">Vagabond dependencies for computation.</param>
/// <param name="procId">Task id for computation.</param>
/// <param name="faultPolicy">Fault policy for computation.</param>
/// <param name="token">Optional cancellation token for computation.</param>
/// <param name="additionalResources">Additional runtime resources supplied by the user.</param>
/// <param name="target">Optional target worker identifier.</param>
/// <param name="computation">Computation to be executed.</param>
let runStartAsCloudProcess (runtime : IRuntimeManager) (parentProc : ICloudProcessEntry option)
                        (dependencies : AssemblyId[]) (taskName : string option)
                        (faultPolicy : IFaultPolicy) (token : ICloudCancellationToken option) 
                        (additionalResources : ResourceRegistry option) (target : IWorkerRef option) 
                        (computation : Cloud<'T>) = async {

    // TODO : some arguments seem to be duplicated when passed either through the parentProc parameter or on their own
    //        this should be resolved when implementing proper cloud process hierarchies

    if not <| FsPickler.IsSerializableType<'T> () then
        let msg = sprintf "Cloud process returns non-serializable type '%s'." Type.prettyPrint<'T>
        return raise <| new SerializationException(msg)
    else

    match ensureSerializable computation with
    | Some e ->
        let msg = sprintf "Cloud process of type '%s' uses non-serializable closure." Type.prettyPrint<'T>
        return raise <| new SerializationException(msg, e)

    | None ->

        // ensure that target worker is valid in current cluster context
        let target = extractWorkerId runtime target

        let! cts = async {
            match token with
            | Some ct -> return! CloudCancellationToken.Create(runtime.CancellationEntryFactory, parents = [|ct|], elevate = true)
            | None -> return! CloudCancellationToken.Create(runtime.CancellationEntryFactory, elevate = true)
        }

        let taskInfo =
            {
                Name = taskName
                CancellationTokenSource = cts
                Dependencies = dependencies
                AdditionalResources = additionalResources
                ReturnTypeName = Type.prettyPrint<'T>
                ReturnType = runtime.Serializer.PickleTyped typeof<'T>
            }

        let! tcs = runtime.ProcessManager.StartProcess taskInfo

        let setResult ctx (result : CloudProcessResult) status = 
            async {
                let currentWorker = ctx.Resources.Resolve<IWorkerId> ()
                match ensureSerializable result with
                | Some e ->
                    let msg = sprintf "Could not serialize result for task '%s' of type '%s'." tcs.Id Type.prettyPrint<'T>
                    let se = new SerializationException(msg, e)
                    let! _ = tcs.TrySetResult(Exception (ExceptionDispatchInfo.Capture se), currentWorker)
                    do! tcs.DeclareStatus CloudProcessStatus.Faulted
                | None ->
                    let! _ = tcs.TrySetResult(result, currentWorker)
                    do! tcs.DeclareStatus status

                cts.Cancel()
                WorkItemExecutionMonitor.TriggerCompletion ctx
            } |> WorkItemExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t) CloudProcessStatus.Completed
        let econt ctx e = setResult ctx (Exception e) CloudProcessStatus.UserException
        let ccont ctx c = setResult ctx (Cancelled c) CloudProcessStatus.Canceled
        let fcont ctx e = setResult ctx (Exception e) CloudProcessStatus.Faulted

        let workItem = CloudWorkItem.Create (tcs, cts, faultPolicy, scont, econt, ccont, CloudWorkItemType.ProcessRoot, computation, fcont = fcont, ?target = target)
        do! runtime.WorkItemQueue.Enqueue(workItem, isClientSideEnqueue = Option.isNone parentProc)
        runtime.SystemLogger.Logf LogLevel.Info "Posted CloudProcess<%s> '%s'." tcs.Info.ReturnTypeName tcs.Id
        return new CloudProcess<'T>(tcs, runtime)
}