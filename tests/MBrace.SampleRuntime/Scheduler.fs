module internal Nessos.MBrace.SampleRuntime.Scheduler

#nowarn "444"

open System

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.PortablePickle
open Nessos.MBrace.SampleRuntime.Actors

[<AutoSerializable(false)>]
type TaskCompletionEvent () = 
    let event = new Event<unit> ()
    member __.Trigger () = event.Trigger()
    member __.Publish = event.Publish
    static member inline OfContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskCompletionEvent>()
    static member inline TriggerContext (ctx : ExecutionContext) = TaskCompletionEvent.OfContext(ctx).Trigger()
    

type Task = 
    {
        Type : Type
        Id : string
        Job : ExecutionContext -> unit
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    static member RunAsync(state : RuntimeState) (task : Task) = async {
        let tce = new TaskCompletionEvent()
        let! awaitHandle = Async.StartChild(Async.AwaitEvent tce.Publish)
        let runtime = new RuntimeProvider(state, task.Id, task.CancellationTokenSource) :> IRuntimeProvider
        let ctx =
            {
                Resources = resource { yield runtime ; yield tce }
                CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
            }

        do task.Job ctx
        return! awaitHandle
    }

and RuntimeState =
    {
        IPEndPoint : System.Net.IPEndPoint
        TaskQueue : Queue<PortablePickle<Task>>
        CancellationTokenManager : CancellationTokenManager
        ResourceFactory : ResourceFactory
    }
with
    static member InitLocal () =
        {
            IPEndPoint = Actor.LocalEndPoint
            TaskQueue = Queue<PortablePickle<Task>>.Init ()
            CancellationTokenManager = CancellationTokenManager.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    member rt.EnqueueTask cts sc ec cc (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let runWith ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartImmediate(wf, cont, ctx)
        
        let task = { Job = runWith ; CancellationTokenSource = cts ; Id = taskId ; Type = typeof<'T> }
        PortablePickle.Pickle task |> rt.TaskQueue.Enqueue

    member rt.TryDequeue () = rt.TaskQueue.TryDequeue() |> Option.map PortablePickle.UnPickle

    member rt.StartAsCell cts (wf : Cloud<'T>) =
        let resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let scont ctx t = resultCell.SetResult (Completed t) |> ignore ; TaskCompletionEvent.TriggerContext ctx
        let econt ctx e = resultCell.SetResult (Exception e) |> ignore ; TaskCompletionEvent.TriggerContext ctx
        let ccont ctx c = resultCell.SetResult (Cancelled c) |> ignore ; TaskCompletionEvent.TriggerContext ctx
        rt.EnqueueTask cts scont econt ccont wf
        resultCell

and Combinators =
    static member Parallel (state : RuntimeState) (cts : DistributedCancellationTokenSource) (computations : seq<Cloud<'T>>) =

        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx e
            | Choice1Of2 [||] -> cont.Success ctx [||]
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] ->
                let cont' = Continuation.map (fun t -> [| t |]) cont
                Cloud.StartImmediate(comp, cont', ctx)

            | Choice1Of2 computations ->

                let results = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
                let innerCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = cts)
                let exceptionLatch = state.ResourceFactory.RequestLatch(0)

                let toParentCancellationToken (childContext : ExecutionContext) =
                    let parentToken = cts.GetLocalCancellationToken()
                    { childContext with CancellationToken = parentToken }

                let onSuccess i ctx (t : 'T) =
                    if results.SetResult(i, t) then
                        cont.Success (toParentCancellationToken ctx) <| results.ToArray()
                    else
                        TaskCompletionEvent.TriggerContext ctx

                let onException ctx e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel()
                        cont.Exception (toParentCancellationToken ctx) e
                    else
                        TaskCompletionEvent.TriggerContext ctx

                let onCancellation ctx c =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Cancellation (toParentCancellationToken ctx) c
                    else 
                        TaskCompletionEvent.TriggerContext ctx

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask innerCts (onSuccess i) onException onCancellation computations.[i]
                    
                TaskCompletionEvent.TriggerContext ctx)

    static member Choice (state : RuntimeState) (cts : DistributedCancellationTokenSource) (computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx e
            | Choice1Of2 [||] -> cont.Success ctx None
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] -> Cloud.StartImmediate(comp, cont, ctx)
            | Choice1Of2 computations ->

                let n = computations.Length // avoid capturing computation array in cont closures
                let innerCts = state.CancellationTokenManager.RequestCancellationTokenSource cts
                let completionLatch = state.ResourceFactory.RequestLatch(0)
                let exceptionLatch = state.ResourceFactory.RequestLatch(0)

                let toParentCancellationToken (childContext : ExecutionContext) =
                    let parentToken = cts.GetLocalCancellationToken()
                    { childContext with CancellationToken = parentToken }

                let onSuccess ctx (topt : 'T option) =
                    if Option.isSome topt then
                        if exceptionLatch.Increment() = 1 then
                            cont.Success (toParentCancellationToken ctx) topt
                        else
                            TaskCompletionEvent.TriggerContext ctx
                    else
                        if completionLatch.Increment () = n then
                            cont.Success (toParentCancellationToken ctx) None
                        else
                            TaskCompletionEvent.TriggerContext ctx

                let onException ctx e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Exception (toParentCancellationToken ctx) e
                    else
                        TaskCompletionEvent.TriggerContext ctx

                let onCancellation ctx c =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        cont.Cancellation (toParentCancellationToken ctx) c
                    else
                        TaskCompletionEvent.TriggerContext ctx

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask innerCts onSuccess onException onCancellation computations.[i]
                    
                TaskCompletionEvent.TriggerContext ctx)

    // timeout?
    static member StartChild (state : RuntimeState) (cts : DistributedCancellationTokenSource) (computation : Cloud<'T>) = cloud {
        let resultCell = state.StartAsCell cts computation
        return cloud { 
            let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
            return result.Value
        }
    }


and RuntimeProvider private (state : RuntimeState, taskId : string, cts, context) =

    new (state, task, cts) = new RuntimeProvider(state, task, cts, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = taskId
        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()

        member __.Logger = raise <| System.NotImplementedException()

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = new RuntimeProvider(state, taskId, cts, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state cts computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state cts computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state cts computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation