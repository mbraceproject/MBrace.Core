namespace Nessos.MBrace

    open System

    type ParallelismContext =
        | Sequential
        | ThreadParallel
//        | Distributed

//    type IScheduler =
//        abstract ParallelismContext : ParallelismContext with get,set
//        abstract Enqueue : ('T -> unit) -> (exn -> unit) 
//                                -> (OperationCanceledException -> unit) 
//                                -> ICancellationTokenSource -> Cloud<'T> -> unit
//
//    type InMemoryScheduler (resources : ResourceResolver) =
//        let mutable mode = ThreadParallel
//        let cts = new System.Threading.CancellationTokenSource()
//
//        let execute sc ec cc (cts : ICancellationTokenSource) (job : Cloud<'T>) =
//            let ct = cts.GetCancellationToken()
//            let ctx = { Resource = resources ; CancellationToken = ct ; scont = sc ; econt = ec ; ccont = cc }
//            Cloud.StartWithContext(job, ctx)
//
//        interface IScheduler with
//            member __.ParallelismContext
//                with get () = mode
//                and set m = mode <- m
//
//            member __.Enqueue sc ec cc cts job =
//                match mode with
//                | ThreadParallel -> Async.Start(async { return execute sc ec cc cts job } |> Async.Catch |> Async.Ignore)
//                | Sequential -> try execute sc ec cc cts job with _ -> ()