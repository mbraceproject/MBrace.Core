namespace Nessos.MBrace

    open System

    type ParallelismContext =
        | Sequential
        | ThreadParallel
//        | Distributed

    type IScheduler =
        abstract ParallelismContext : ParallelismContext with get,set
        abstract Enqueue : (unit -> unit) -> unit

    type InMemoryScheduler () =
        let mutable mode = ThreadParallel

        interface IScheduler with
            member __.ParallelismContext
                with get () = mode
                and set m = mode <- m

            member __.Enqueue job =
                match mode with
                | ThreadParallel -> Async.Start(async { return job () } |> Async.Catch |> Async.Ignore)
                | Sequential -> try job () with _ -> ()