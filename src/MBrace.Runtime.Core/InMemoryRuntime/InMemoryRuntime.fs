namespace MBrace.Runtime.InMemory

open MBrace
open MBrace.Continuation
open MBrace.Runtime.Logging

/// .NET ThreadPool runtime provider
type ThreadPoolRuntime private (context : SchedulingContext, faultPolicy : FaultPolicy, logger : ICloudLogger) =

    let taskId = System.Guid.NewGuid().ToString()

    /// <summary>
    ///     Creates a new threadpool runtime instance.
    /// </summary>
    /// <param name="logger">Logger for runtime. Defaults to no logging.</param>
    /// <param name="faultPolicy">Fault policy for runtime. Defaults to Infinite retries.</param>
    static member Create (?logger : ICloudLogger, ?faultPolicy) = 
        let logger = match logger with Some l -> l | None -> new NullLogger() :> _
        let faultPolicy = match faultPolicy with Some f -> f | None -> FaultPolicy.InfiniteRetry()
        new ThreadPoolRuntime(ThreadParallel, faultPolicy, logger)
        
    interface IRuntimeProvider with
        member __.ProcessId = sprintf "In-Memory cloud process (pid:%d)" <| System.Diagnostics.Process.GetCurrentProcess().Id
        member __.TaskId = taskId
        member __.Logger = logger
        member __.IsTargetedWorkerSupported = false
        member __.GetAvailableWorkers () = async {
            return raise <| new System.NotSupportedException("'GetAvailableWorkers' not supported in InMemory runtime.")
        }

        member __.CurrentWorker = raise <| new System.NotSupportedException("'CurrentWorker' not supported in InMemory runtime.")

        member __.SchedulingContext = context
        member __.WithSchedulingContext newContext = 
            let newContext =
                match context, newContext with
                | Sequential, ThreadParallel -> invalidOp <| sprintf "Cannot set scheduling context from '%A' to '%A'." Sequential ThreadParallel
                | _, Distributed -> ThreadParallel
                | _, c -> c

            new ThreadPoolRuntime(newContext, faultPolicy, logger) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newFp = new ThreadPoolRuntime(context, newFp, logger) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            let computations =
                computations
                |> Seq.map (fun (c,w) ->
                    if Option.isSome w then
                        raise <| new System.NotSupportedException("Targeted workers not supported in InMemory runtime.")
                    else
                        c)
                |> Seq.toArray

            match context with
            | Sequential -> Sequential.Parallel computations
            | _ -> ThreadPool.Parallel computations

        member __.ScheduleChoice computations = 
            let computations =
                computations
                |> Seq.map (fun (c,w) ->
                    if Option.isSome w then
                        raise <| new System.NotSupportedException("Targeted workers not supported in In-Memory runtime.")
                    else
                        c)
                |> Seq.toArray

            match context with
            | Sequential -> Sequential.Choice computations
            | _ -> ThreadPool.Choice computations

        member __.ScheduleStartChild (workflow, ?target:IWorkerRef, ?timeoutMilliseconds:int) = 
            match context with
            | Sequential -> Sequential.StartChild workflow
            | _ -> ThreadPool.StartChild(workflow, ?timeoutMilliseconds = timeoutMilliseconds)