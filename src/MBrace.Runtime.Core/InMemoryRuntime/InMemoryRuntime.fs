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
        member __.ProcessId = "in memory process"
        member __.TaskId = taskId
        member __.Logger = logger
        member __.GetAvailableWorkers () = async {
            return raise <| new System.NotSupportedException("'GetAvailableWorkers' not supported in InMemory runtime.")
        }

        member __.CurrentWorker = raise <| new System.NotSupportedException("'CurrentWorker' not supported in InMemory runtime.")

        member __.SchedulingContext = context
        member __.WithSchedulingContext newContext = 
            let newContext =
                match newContext with
                | Distributed -> ThreadParallel
                | c -> c

            new ThreadPoolRuntime(newContext, faultPolicy, logger) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newFp = new ThreadPoolRuntime(context, newFp, logger) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Sequential -> Sequential.Parallel computations
            | _ -> ThreadPool.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Sequential -> Sequential.Choice computations
            | _ -> ThreadPool.Choice computations

        member __.ScheduleStartChild (workflow, ?target:IWorkerRef, ?timeoutMilliseconds:int) = 
            match context with
            | Sequential -> Sequential.StartChild workflow
            | _ -> ThreadPool.StartChild(workflow, ?timeoutMilliseconds = timeoutMilliseconds)

type InMemory =

    /// <summary>
    ///     Creates a resource bundle for in-memory cloud computations.
    /// </summary>
    /// <param name="logger">Logger for runtime. Defaults to no logging.</param>
    static member CreateResources(?logger : ICloudLogger) : ResourceRegistry =
        resource { 
            yield ThreadPoolRuntime.Create (?logger = logger) :> IRuntimeProvider
            yield InMemoryChannelProvider.CreateConfiguration()
            yield InMemoryAtomProvider.CreateConfiguration()
        }