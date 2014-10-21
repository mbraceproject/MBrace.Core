module Nessos.MBrace.Remote.Scheduler

    #nowarn "444"

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Remote.Gadgets
    open Nessos.MBrace.Remote.Vagrant

    type RuntimeState =
        {
            TaskQueue : Queue<PortableWorkflow>
            ResourceManager : ResourceManager
        }
    with
        static member InitLocal () =
            {
                TaskQueue = Queue<PortableWorkflow>.Init ()
                ResourceManager = ResourceManager.Init ()
            }

//    let Parallel (state : RuntimeState) (computations : seq<Cloud<'T>>) =
//        Cloud.FromContinuations(fun ctx ->
//            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
//            | Choice2Of2 e -> ctx.econt e
//            | Choice1Of2 computations ->
//                if computations.Length = 0 then ctx.scont [||] else
//                    
//                let n = computations.Length
//                let results = state.ResourceManager.RequestResultAggregator<'T>(n)
////                let innerCts = mkLinkedCts ctx.CancellationToken
//                let exceptionLatch = state.ResourceManager.RequestLatch()
//
//                let onSuccess i (t : 'T) =
//                    if results.SetResult(i, t) then
//                        ctx.scont <| results.ToArray()
//
//                let onException e =
//                    if exceptionLatch.Increment() = 1 then
////                        innerCts.Cancel ()
//                        ctx.econt e
//
//                let onCancellation ce =
//                    if exceptionLatch.Increment() = 1 then
////                        innerCts.Cancel ()
//                        ctx.ccont ce
//
//                for i = 0 to computations.Length - 1 do
//                    scheduleTask ctx.Resource (onSuccess i) onException onCancellation innerCts.Token computations.[i])

//    /// <summary>
//    ///     Provides a context-less Cloud.Choice implementation
//    ///     for execution within the thread pool.
//    /// </summary>
//    /// <param name="computations">Input computations</param>
//    static member Choice(computations : seq<Cloud<'T option>>) =
//        Cloud.FromContinuations(fun ctx ->
//            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
//            | Choice2Of2 e -> ctx.econt e
//            | Choice1Of2 computations ->
//                if computations.Length = 0 then ctx.scont None else
//
//                let innerCts = mkLinkedCts ctx.CancellationToken
//                let completionLatch = new Latch(0)
//                let exceptionLatch = new Latch(0)
//
//                let onSuccess (topt : 'T option) =
//                    if Option.isSome topt then
//                        if exceptionLatch.Increment() = 1 then
//                            ctx.scont topt
//                    else
//                        if completionLatch.Increment () = computations.Length then
//                            ctx.scont None
//
//                let onException e =
//                    if exceptionLatch.Increment() = 1 then
//                        innerCts.Cancel ()
//                        ctx.econt e
//
//                let onCancellation ce =
//                    if exceptionLatch.Increment() = 1 then
//                        innerCts.Cancel ()
//                        ctx.ccont ce
//
//                for i = 0 to computations.Length - 1 do
//                    scheduleTask ctx.Resource onSuccess onException onCancellation innerCts.Token computations.[i])
//
//
//    /// <summary>
//    ///     Provides a context-less Cloud.StartChild implementation
//    ///     for execution within the thread pool.
//    /// </summary>
//    /// <param name="computation">Input computation.</param>
//    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
//    static member StartChild (computation : Cloud<'T>, ?timeoutMilliseconds) : Cloud<Cloud<'T>> = cloud {
//        let! resource = Cloud.GetResourceRegistry()
//        let asyncWorkflow = Cloud.ToAsync(computation, resources = resource)
//        let! chWorkflow = Cloud.OfAsync <| Async.StartChild(asyncWorkflow, ?millisecondsTimeout = timeoutMilliseconds)
//        return Cloud.OfAsync chWorkflow
//    }