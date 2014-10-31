module internal Nessos.MBrace.SampleRuntime.Scheduler

#nowarn "444"

open System

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.Actors
open Nessos.MBrace.SampleRuntime.Vagrant

[<AutoSerializable(false)>]
type TaskCompletionEvent () = 
    inherit Event<exn option> ()
    static member inline OfContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskCompletionEvent>()
    static member inline TriggerCompletion ctx = TaskCompletionEvent.OfContext(ctx).Trigger None
    static member inline TriggerFault ctx e = TaskCompletionEvent.OfContext(ctx).Trigger(Some e)
    

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
        let runtime = new RuntimeProvider(state, task.Id) :> IRuntimeProvider
        let ctx =
            {
                Resources = resource { yield runtime ; yield tce ; yield task.CancellationTokenSource }
                CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
            }

        do task.Job ctx
        let! result = awaitHandle
        match result with
        | None -> ()
        | Some e -> return! Async.Reraise e
    }

and RuntimeState =
    {
        IPEndPoint : System.Net.IPEndPoint
        TaskQueue : Queue<PortablePickle<Task>>
        AssemblyExporter : AssemblyExporter
        CancellationTokenManager : CancellationTokenManager
        ResourceFactory : ResourceFactory
    }
with
    static member InitLocal () =
        {
            IPEndPoint = Actor.LocalEndPoint
            TaskQueue = Queue<PortablePickle<Task>>.Init ()
            AssemblyExporter = AssemblyExporter.Init()
            CancellationTokenManager = CancellationTokenManager.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    member rt.EnqueueTask cts sc ec cc (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let runWith ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartImmediate(wf, cont, ctx)
        
        let task = { Job = runWith ; CancellationTokenSource = cts ; Id = taskId ; Type = typeof<'T> }
        PortablePickle.pickle task |> rt.TaskQueue.Enqueue

    member rt.TryDequeue () = async {
        match rt.TaskQueue.TryDequeue()  with
        | None -> return None
        | Some taskP -> 
            let! task = PortablePickle.unpickle rt.AssemblyExporter taskP
            return Some task
    }

    member rt.StartAsCell cts (wf : Cloud<'T>) =
        let resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            match (try resultCell.SetResult r |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice1Of2 true -> TaskCompletionEvent.TriggerCompletion ctx
            | Choice1Of2 false -> TaskCompletionEvent.TriggerFault ctx (new Exception("Could not set result."))
            | Choice2Of2 e -> TaskCompletionEvent.TriggerFault ctx e

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueTask cts scont econt ccont wf
        resultCell

and Combinators private () =
    static let updateCts (cts : DistributedCancellationTokenSource) (ctx : ExecutionContext) =
        let token = cts.GetLocalCancellationToken()
        { Resources = ctx.Resources.Register(cts) ; CancellationToken = token }

    static let guard ctx (f : unit -> 'T) =
        try f () |> Some with e -> TaskCompletionEvent.TriggerFault ctx e ; None

    static let guardedFromContinuations F =
        Cloud.FromContinuations(fun ctx cont ->
            match (try F ctx cont ; None with e -> Some e) with
            | None -> ()
            | Some e -> TaskCompletionEvent.TriggerFault ctx e)
        
    static member Parallel (state : RuntimeState) (computations : seq<Cloud<'T>>) =
        guardedFromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx e
            | Choice1Of2 [||] -> cont.Success ctx [||]
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] ->
                let cont' = Continuation.map (fun t -> [| t |]) cont
                Cloud.StartImmediate(comp, cont', ctx)

            | Choice1Of2 computations ->

                let parentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource> ()
                let childCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = parentCts)
                let results = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
                let cancellationLatch = state.ResourceFactory.RequestLatch(0)

                let onSuccess i ctx (t : 'T) =
                    let completedResults = 
                        guard ctx (fun () ->
                            if results.SetResult(i, t) then Some <| results.ToArray ()
                            else None)

                    match completedResults with
                    | Some(Some results) -> cont.Success (updateCts parentCts ctx) results
                    | Some None -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

                let onException ctx e =
                    let isAcquiredCancellation =
                        guard ctx (fun () ->
                            if cancellationLatch.Increment() = 1 then
                                childCts.Cancel() ; true
                            else
                                false)

                    match isAcquiredCancellation with
                    | Some true -> cont.Exception (updateCts parentCts ctx) e
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

                let onCancellation ctx c =
                    let isAcquiredCancellation =
                        guard ctx (fun () ->
                            if cancellationLatch.Increment() = 1 then
                                childCts.Cancel () ; true
                            else
                                false)

                    match isAcquiredCancellation with
                    | Some true -> cont.Cancellation (updateCts parentCts ctx) c
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask childCts (onSuccess i) onException onCancellation computations.[i]
                    
                TaskCompletionEvent.TriggerCompletion ctx)

    static member Choice (state : RuntimeState) (computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> cont.Exception ctx e
            | Choice1Of2 [||] -> cont.Success ctx None
            // schedule single-child parallel workflows in current task
            // note that this invalidates expected workflow semantics w.r.t. mutability.
            | Choice1Of2 [| comp |] -> Cloud.StartImmediate(comp, cont, ctx)
            | Choice1Of2 computations ->

                let n = computations.Length // avoid capturing computation array in cont closures
                let parentCts = ctx.Resources.Resolve<DistributedCancellationTokenSource>()
                let childCts = state.CancellationTokenManager.RequestCancellationTokenSource parentCts
                let completionLatch = state.ResourceFactory.RequestLatch(0)
                let cancellationLatch = state.ResourceFactory.RequestLatch(0)

                let onSuccess ctx (topt : 'T option) =
                    if Option.isSome topt then
                        let isAcquiredCancellation =
                            guard ctx (fun () ->
                                if cancellationLatch.Increment() = 1 then
                                    childCts.Cancel () ; true
                                else
                                    false)

                        match isAcquiredCancellation with
                        | Some true -> cont.Success (updateCts parentCts ctx) topt
                        | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                        | None -> ()
                    else
                        let isCompleted = guard ctx (fun () -> completionLatch.Increment () = n)

                        match isCompleted with
                        | Some true -> cont.Success (updateCts parentCts ctx) None
                        | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                        | None -> ()

                let onException ctx e =
                    let isAcquiredCancellation =
                        guard ctx (fun () ->
                            if cancellationLatch.Increment() = 1 then
                                childCts.Cancel () ; true
                            else
                                false)

                    match isAcquiredCancellation with
                    | Some true -> cont.Exception (updateCts parentCts ctx) e
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

                let onCancellation ctx c =
                    let isAcquiredCancellation =
                        guard ctx (fun () ->
                            if cancellationLatch.Increment() = 1 then
                                childCts.Cancel () ; true
                            else
                                false)

                    match isAcquiredCancellation with
                    | Some true -> cont.Cancellation (updateCts parentCts ctx) c
                    | Some false -> TaskCompletionEvent.TriggerCompletion ctx
                    | None -> ()

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask childCts onSuccess onException onCancellation computations.[i]
                    
                TaskCompletionEvent.TriggerCompletion ctx)

    // timeout?
    static member StartChild (state : RuntimeState) (computation : Cloud<'T>) = cloud {
        let! cts = Cloud.GetResource<DistributedCancellationTokenSource> ()
        let resultCell = state.StartAsCell cts computation
        return cloud { 
            let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
            return result.Value
        }
    }


and RuntimeProvider private (state : RuntimeState, taskId : string, context) =

    new (state, task) = new RuntimeProvider(state, task, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = taskId
        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()

        member __.Logger = raise <| System.NotImplementedException()

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = new RuntimeProvider(state, taskId, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation