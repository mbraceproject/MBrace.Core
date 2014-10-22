module internal Nessos.MBrace.SampleRuntime.Scheduler

#nowarn "444"

open System

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.PortablePickle
open Nessos.MBrace.SampleRuntime.Actors

type TaskCompletionEvent = EventRef<unit>

type Task = 
    {
        Type : System.Type
        TaskId : string
        TaskCompletionEvent : TaskCompletionEvent
        Job : ResourceRegistry -> CancellationTokenSource -> unit
        CancellationTokenSource : CancellationTokenSource
    }
with
    static member RunAsync(state : RuntimeState) (task : Task) = async {
        use uninst = task.TaskCompletionEvent.InstallLocalEvent()
        let! awaitHandle = Async.StartChild(Async.AwaitEvent task.TaskCompletionEvent.Publish)
        let runtime = new RuntimeProvider(state, task) :> IRuntimeProvider
        let res = resource { yield runtime }
        do task.Job res task.CancellationTokenSource
        do! awaitHandle
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

    member rt.EnqueueTask sc ec cc cts tce (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let runTask resource (cts:CancellationTokenSource) =
            let token = cts.GetLocalCancellationToken()
            let ctx = { Resource = resource ; scont = sc ; econt = ec ; ccont = cc ; CancellationToken = token }
            Cloud.StartImmediate(wf, ctx)
        
        let task = { Job = runTask ; CancellationTokenSource = cts ; TaskId = taskId ; Type = typeof<'T> ; TaskCompletionEvent = tce }
        PortablePickle.Pickle task |> rt.TaskQueue.Enqueue

    member rt.TryDequeue () = rt.TaskQueue.TryDequeue() |> Option.map PortablePickle.UnPickle

    member rt.StartAsCell cts (wf : Cloud<'T>) =
        let resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let taskCompletionEvent = new TaskCompletionEvent()
        // defines root continuations for this process ; task completion event triggered by default
        let scont t = resultCell.SetResult (Completed t) |> ignore ; taskCompletionEvent.Trigger() 
        let econt e = resultCell.SetResult (Exception e) |> ignore ; taskCompletionEvent.Trigger()
        let ccont c = resultCell.SetResult (Cancelled c) |> ignore ; taskCompletionEvent.Trigger()
        rt.EnqueueTask scont econt ccont cts taskCompletionEvent wf
        resultCell

and Combinators =
    static member Parallel (state : RuntimeState) (cts : CancellationTokenSource) (tce : TaskCompletionEvent) (computations : seq<Cloud<'T>>) =
        Cloud.FromContinuations(fun ctx ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> ctx.econt e
            | Choice1Of2 [||] -> ctx.scont [||]
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] ->
                let ctx' = Context.map (fun t -> [| t |]) ctx
                Cloud.StartImmediate(comp, ctx')

            | Choice1Of2 computations ->
                let scont = ctx.scont
                let econt = ctx.econt
                let ccont = ctx.ccont
                let results = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
                let innerCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = cts)
                let exceptionLatch = state.ResourceFactory.RequestLatch(0)

                let onSuccess i (t : 'T) =
                    if results.SetResult(i, t) then
                        scont <| results.ToArray()
                    else
                        tce.Trigger()

                let onException e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel()
                        econt e
                    else
                        tce.Trigger()

                let onCancellation ce =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        ccont ce
                    else 
                        tce.Trigger()

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask (onSuccess i) onException onCancellation innerCts tce computations.[i]
                    
                tce.Trigger())

    static member Choice (state : RuntimeState) (cts : CancellationTokenSource) (tce : TaskCompletionEvent) (computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> ctx.econt e
            | Choice1Of2 [||] -> ctx.scont None
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] -> Cloud.StartImmediate(comp, ctx)
            | Choice1Of2 computations ->

                let n = computations.Length
                let scont = ctx.scont
                let econt = ctx.econt
                let ccont = ctx.ccont
                let innerCts = state.CancellationTokenManager.RequestCancellationTokenSource cts
                let completionLatch = state.ResourceFactory.RequestLatch(0)
                let exceptionLatch = state.ResourceFactory.RequestLatch(0)

                let onSuccess (topt : 'T option) =
                    if Option.isSome topt then
                        if exceptionLatch.Increment() = 1 then
                            scont topt
                        else
                            tce.Trigger()
                    else
                        if completionLatch.Increment () = n then
                            scont None
                        else
                            tce.Trigger()

                let onException e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        econt e
                    else
                        tce.Trigger()

                let onCancellation ce =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        ccont ce
                    else
                        tce.Trigger()

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask onSuccess onException onCancellation innerCts tce computations.[i]
                    
                tce.Trigger())

    // timeout?
    static member StartChild (state : RuntimeState) (cts : CancellationTokenSource) (computation : Cloud<'T>) = cloud {
        let resultCell = state.StartAsCell cts computation
        return cloud { 
            let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
            return result.Value
        }
    }


and RuntimeProvider private (state : RuntimeState, task : Task, context) =

    new (state, task) = new RuntimeProvider(state, task, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = task.TaskId
        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()

        member __.Logger = raise <| System.NotImplementedException()

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = new RuntimeProvider(state, task, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state task.CancellationTokenSource task.TaskCompletionEvent computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state task.CancellationTokenSource task.TaskCompletionEvent computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state task.CancellationTokenSource computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation