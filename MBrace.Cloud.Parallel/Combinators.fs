namespace Nessos.MBrace

    #nowarn "444"

    open System.Threading

    open Nessos.MBrace.Runtime

    type Cloud =

        static member Parallel (computations : seq<Cloud<'T>>) = cloud {
            let! scheduler = Cloud.GetResource<IDistributionProvider> ()
            return! scheduler.Parallel computations
        }

        static member Choice (computations : seq<Cloud<'T option>>) = cloud {
            let! scheduler = Cloud.GetResource<IDistributionProvider> ()
            return! scheduler.Choice computations
        }

        static member SendToWorker(worker : IWorkerRef, computation : Cloud<'T>) = cloud {
            let! scheduler = Cloud.GetResource<IDistributionProvider> ()
            return! scheduler.SendToWorker worker computation
        }

        static member GetRuntimeInfo () = cloud {
            let! infoProvider = Cloud.GetResource<IRuntimeInfoProvider> ()
            return! Cloud.OfAsync <| infoProvider.GetRuntimeInfo()
        }

        static member GetWorkerCount () = cloud {
            let! info = Cloud.GetRuntimeInfo()
            return info.Cluster.Length
        }


    type CloudRef =
        
        static member New(t : 'T) = cloud {
            let! storageProvider = Cloud.GetResource<IStorageProvider> ()
            return! Cloud.OfAsync <| storageProvider.CreateCloudRef t
        }
            

//        /// <summary>
//        ///     Asynchronously runs a cloud computation with given scheduler implementation.
//        /// </summary>
//        /// <param name="computation">Computation to be executed.</param>
//        /// <param name="scheduler">Scheduling context; defaults to InMemoryScheduler.</param>
//        /// <param name="cancellationToken">Cancellation token.</param>
//        static member RunWithSchedulerAsync(computation : Cloud<'T>, ?scheduler : IScheduler, ?cancellationToken : CancellationToken) : Async<'T> =
//            async {
//                let scheduler = match scheduler with None -> new InMemoryScheduler() :> IScheduler | Some sched -> sched
//                try return! scheduler.ScheduleAsTask(computation, ?cancellationToken = cancellationToken) |> Async.AwaitTask
//                with :? System.AggregateException as e -> return raise e.InnerException
//            }
//
//        /// <summary>
//        ///     Runs a cloud computation with given scheduler implementation.
//        /// </summary>
//        /// <param name="computation">Computation to be executed.</param>
//        /// <param name="scheduler">Scheduling context; defaults to InMemoryScheduler.</param>
//        /// <param name="cancellationToken">Cancellation token.</param>
//        static member RunWithScheduler(computation : Cloud<'T>, ?scheduler : IScheduler, ?cancellationToken : CancellationToken) : 'T =
//            Cloud.RunWithSchedulerAsync(computation, ?scheduler = scheduler, ?cancellationToken = cancellationToken)
//            |> Async.RunSynchronously
//        
//        /// <summary>
//        ///     Cloud.Parallel combinator
//        /// </summary>
//        /// <param name="computations">Input computations to be executed in parallel.</param>
//        static member Parallel (computations : seq<Cloud<'T>>) : Cloud<'T []> =
//            Cloud.FromContinuations(fun ctx ->
//                let initialization =
//                    try
//                        let computations = Seq.toArray computations
//                        if computations.Length = 0 then Choice1Of3 () else
//
//                        let scheduler = ctx.Resource.Resolve<IScheduler> ()
//                        let aggregatorFactory = ctx.Resource.Resolve<IResultAggregatorFactory> ()
//                        let aggregator = aggregatorFactory.Create<'T>(computations.Length)
//
//                        let cts = ctx.Resource.Resolve<ICancellationTokenSource> ()
//                        let innerCts = cts.CreateLinkedCancellationTokenSource ()
//
//                        let latchFactory = ctx.Resource.Resolve<ILatchFactory> ()
//                        let cancellationLatch = latchFactory.Create 0
//
//                        Choice2Of3(computations, scheduler, aggregator, innerCts, cancellationLatch)
//                    with e -> Choice3Of3 e
//
//                match initialization with
//                | Choice1Of3 () -> ctx.scont [||]
//                | Choice3Of3 e -> ctx.econt e
//                | Choice2Of3(computations, scheduler, aggregator, innerCts, cancellationLatch) ->
//                    // avoid capturing contextual data in closures
//                    let scont = ctx.scont
//                    let econt = ctx.econt
//                    let ccont = ctx.ccont
//
//                    let onSuccess i (t : 'T) =
//                        let isCompleted = aggregator.SetResult(i, t)
//                        if isCompleted then
//                            scont <| aggregator.ToArray()
//
//                    let onException e =
//                        if cancellationLatch.Incr () = 1 then
//                            innerCts.Cancel ()
//                            econt e
//
//                    let onCancellation ce =
//                        if cancellationLatch.Incr () = 1 then
//                            innerCts.Cancel ()
//                            ccont ce
//                        
//                    for i = 0 to computations.Length - 1 do
//                        scheduler.Enqueue (onSuccess i) onException onCancellation innerCts computations.[i])
//
//        /// <summary>
//        ///     Cloud.Choice combinator
//        /// </summary>
//        /// <param name="computations">Input computations to be executed in parallel.</param>
//        static member Choice (computations : seq<Cloud<'T option>>) : Cloud<'T option> =
//            Cloud.FromContinuations(fun ctx ->
//                let initialization =
//                    try
//                        let computations = Seq.toArray computations
//                        if computations.Length = 0 then Choice1Of3 () else
//
//                        let scheduler = ctx.Resource.Resolve<IScheduler> ()
//
//                        let cts = ctx.Resource.Resolve<ICancellationTokenSource> ()
//                        let innerCts = cts.CreateLinkedCancellationTokenSource ()
//
//                        let latchFactory = ctx.Resource.Resolve<ILatchFactory> ()
//                        let failedLatch = latchFactory.Create 0
//                        let resultLatch = latchFactory.Create 0
//
//                        Choice2Of3(computations, scheduler, innerCts, failedLatch, resultLatch)
//                    with e -> Choice3Of3 e
//
//                match initialization with
//                | Choice1Of3 () -> ctx.scont None
//                | Choice3Of3 e -> ctx.econt e
//                | Choice2Of3(computations, scheduler, innerCts, failedLatch, resultLatch) ->
//                    // avoid capturing contextual data in closures
//                    let scont = ctx.scont
//                    let econt = ctx.econt
//                    let ccont = ctx.ccont
//                    let n = computations.Length
//
//                    let onSuccess (topt : 'T option) =
//                        match topt with
//                        | None when failedLatch.Incr() = n -> scont topt
//                        | Some _ when resultLatch.Incr() = 1 -> scont topt
//                        | _ -> ()
//
//                    let onException e =
//                        if resultLatch.Incr () = 1 then
//                            innerCts.Cancel ()
//                            econt e
//
//                    let onCancellation ce =
//                        if resultLatch.Incr () = 1 then
//                            innerCts.Cancel ()
//                            ccont ce
//                        
//                    for i = 0 to computations.Length - 1 do
//                        scheduler.Enqueue onSuccess onException onCancellation innerCts computations.[i])


//        /// Force sequential evaluation semantics in scheduler;
//        /// this implementation is obviously flawed, just a proof of concept
//        static member ToSequential(workflow : Cloud<'T>) : Cloud<'T> = cloud {
//            let! scheduler = Cloud.GetResource<IScheduler> ()
//            let pc = scheduler.Context
//            scheduler.Context <- Sequential
//            try return! workflow
//            finally
//                scheduler.Context <- pc
//        }