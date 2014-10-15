namespace Nessos.MBrace.Runtime

    open System.Threading
    open System.Threading.Tasks

    open Nessos.MBrace

    //------------------------------------------------------------------------//
    //  Collection of sample in-memory implementations for runtime resources  //
    //------------------------------------------------------------------------//

    type InMemoryCancellationTokenSource (?cancellationToken : CancellationToken) =

        let localCts = new CancellationTokenSource()
        let linkedCts = 
            match cancellationToken with
            | None -> localCts
            | Some ct -> CancellationTokenSource.CreateLinkedTokenSource(ct, localCts.Token)

        interface ICancellationTokenSource with
            member __.Cancel () = localCts.Cancel()
            member __.GetCancellationToken () = linkedCts.Token
            member __.CreateLinkedCancellationTokenSource () = 
                new InMemoryCancellationTokenSource(linkedCts.Token) :> ICancellationTokenSource

    type InMemoryLatch (init : int) =
        [<VolatileField>]
        let mutable flag = init

        interface ILatch with
            member __.Incr () = Interlocked.Increment &flag
            member __.Decr () = Interlocked.Decrement &flag
            member __.Value = flag

    and InMemoryLatchFactory () =
        interface ILatchFactory with
            member __.Create init = new InMemoryLatch(init) :> ILatch


    type InMemoryAggregator<'T> (capacity : int) =
        [<VolatileField>]
        let mutable counter = 0
        let array = Array.zeroCreate<'T> capacity

        interface IResultAggregator<'T> with
            member __.SetResult(i, t) = array.[i] <- t ; System.Threading.Interlocked.Increment &counter = capacity
            member __.Capacity = capacity
            member __.Count = counter
            member __.ToArray () = array

    and InMemoryAggregatorFactory() =
        interface IResultAggregatorFactory with
            member __.Create<'T> capacity = new InMemoryAggregator<'T>(capacity) :> IResultAggregator<'T>


    type InMemoryScheduler () as self =
        let mutable mode = Threadpool

        let resourceFactory =
            ResourceResolverFactory.Empty
                .Register<IScheduler>(self :> IScheduler)
                .Register<ILatchFactory>(new InMemoryLatchFactory() :> ILatchFactory)
                .Register<IResultAggregatorFactory>(new InMemoryAggregatorFactory() :> IResultAggregatorFactory)

        let execute sc ec cc (cts : ICancellationTokenSource) (job : Cloud<'T>) =
            let resources = resourceFactory.Register(cts).GetResolver()
            let ct = cts.GetCancellationToken()
            let ctx = { Resource = resources ; CancellationToken = ct ; scont = sc ; econt = ec ; ccont = cc }
            Cloud.StartWithContext(job, ctx)

        interface IScheduler with
            member __.Context
                with get () = mode
                and set m = mode <- m

            member __.Enqueue sc ec cc cts job =
                match mode with
                | Threadpool -> Task.Factory.StartNew(fun () -> execute sc ec cc cts job) |> ignore
                | Sequential -> try execute sc ec cc cts job with _ -> ()
        
            member self.ScheduleAsTask(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) : Task<'T> =
                let tcs = new TaskCompletionSource<'T>()
                let cts = new InMemoryCancellationTokenSource(?cancellationToken = cancellationToken) :> ICancellationTokenSource
                execute tcs.SetResult tcs.SetException (fun _ -> tcs.SetCanceled ()) cts workflow
                tcs.Task