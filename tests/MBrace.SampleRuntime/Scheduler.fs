module internal Nessos.MBrace.SampleRuntime.Scheduler

#nowarn "444"

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.Actors
open Nessos.MBrace.SampleRuntime.Vagrant

type Task = 
    {
        Type : System.Type
        TaskId : string
        Job : ResourceRegistry -> CancellationTokenSource -> unit
        CancellationTokenSource : CancellationTokenSource
    }
with
    static member Run(state : RuntimeState) (task : Task) =
        let runtime = new RuntimeProvider(state, task.CancellationTokenSource, task.TaskId) :> IRuntimeProvider
        let res = resource { yield runtime }
        task.Job res task.CancellationTokenSource

and RuntimeState =
    {
        TaskQueue : Queue<PortablePickle<Task>>
        CancellationTokenManager : CancellationTokenManager
        ResourceFactory : ResourceFactory
    }
with
    static member InitLocal () =
        {
            TaskQueue = Queue<PortablePickle<Task>>.Init ()
            CancellationTokenManager = CancellationTokenManager.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    member rt.EnqueueTask sc ec cc cts (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let runTask resource (cts:CancellationTokenSource) =
            let token = cts.GetLocalCancellationToken()
            let ctx = { Resource = resource ; scont = sc ; econt = ec ; ccont = cc ; CancellationToken = token }
            Cloud.StartImmediate(wf, ctx)
        
        let task = { Job = runTask ; CancellationTokenSource = cts ; TaskId = taskId ; Type = typeof<'T> }
        PortablePickle.pickle task |> rt.TaskQueue.Enqueue

    member rt.TryDequeue () = rt.TaskQueue.TryDequeue() |> Option.map PortablePickle.unpickle

    member rt.StartAsCell cts (wf : Cloud<'T>) =
        let resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let scont t = resultCell.SetResult (Completed t) |> ignore
        let econt e = resultCell.SetResult (Exception e) |> ignore
        let ccont c = resultCell.SetResult (Cancelled c) |> ignore
        rt.EnqueueTask scont econt ccont cts wf
        resultCell

and Combinators =
    static member Parallel (state : RuntimeState) (cts : CancellationTokenSource) (computations : seq<Cloud<'T>>) =
        Cloud.FromContinuations(fun ctx ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> ctx.econt e
            | Choice1Of2 computations ->
                if computations.Length = 0 then ctx.scont [||] else

                let scont = ctx.scont
                let econt = ctx.econt
                let ccont = ctx.ccont
                let results = state.ResourceFactory.RequestResultAggregator<'T>(computations.Length)
                let innerCts = state.CancellationTokenManager.RequestCancellationTokenSource(parent = cts)
                let exceptionLatch = state.ResourceFactory.RequestLatch(0)

                let onSuccess i (t : 'T) =
                    if results.SetResult(i, t) then
                        scont <| results.ToArray()

                let onException e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel()
                        econt e

                let onCancellation ce =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        ccont ce

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask (onSuccess i) onException onCancellation innerCts computations.[i])

    static member Choice (state : RuntimeState) (cts : CancellationTokenSource) (computations : seq<Cloud<'T option>>) =
        Cloud.FromContinuations(fun ctx ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice2Of2 e -> ctx.econt e
            | Choice1Of2 computations ->
                if computations.Length = 0 then ctx.scont None else

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
                        if completionLatch.Increment () = n then
                            scont None

                let onException e =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        econt e

                let onCancellation ce =
                    if exceptionLatch.Increment() = 1 then
                        innerCts.Cancel ()
                        ccont ce

                for i = 0 to computations.Length - 1 do
                    state.EnqueueTask onSuccess onException onCancellation innerCts computations.[i])

    // timeout?
    static member StartChild (state : RuntimeState) (cts : CancellationTokenSource) (computation : Cloud<'T>) = cloud {
        let resultCell = state.StartAsCell cts computation
        return cloud { 
            let! result = Cloud.OfAsync <| resultCell.AwaitResult() 
            return result.Value
        }
    }


and RuntimeProvider private (state : RuntimeState, cts : CancellationTokenSource, taskId, context) =

    new (state, cts, taskId) = new RuntimeProvider(state, cts, taskId, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = taskId
        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()

        member __.Logger = raise <| System.NotImplementedException()

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = new RuntimeProvider(state, cts, taskId, context) :> IRuntimeProvider

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