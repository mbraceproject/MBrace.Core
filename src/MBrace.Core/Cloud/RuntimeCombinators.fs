// Collection of runtime-related combinators
[<AutoOpen>]
module Nessos.MBrace.RuntimeCombinators

#nowarn "444"

open System.Threading
open System.Threading.Tasks

open Nessos.MBrace.Continuation

type Nessos.MBrace.Cloud with

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Log(logEntry : string) : Cloud<unit> = cloud {
        let! runtime = Cloud.TryGetResource<IRuntimeProvider> ()
        return 
            match runtime with
            | None -> ()
            | Some r -> r.Logger.Log logEntry
    }

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Logf fmt = Printf.ksprintf Cloud.Log fmt

    /// <summary>
    ///     Cloud.Parallel combinator
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Cloud<'T>>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let workflow = runtime.ScheduleParallel computations
        return! Cloud.WithAppendedStackTrace "Cloud.Parallel[T](seq<Cloud<T>> computations)" workflow
    }

    /// <summary>
    ///     Cloud.Choice combinator
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<Cloud<'T option>>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let workflow = runtime.ScheduleChoice computations
        return! Cloud.WithAppendedStackTrace "Cloud.Choice[T](seq<Cloud<T option>> computations)" workflow
    }

    /// <summary>
    ///     Start cloud computation as child. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds; defaults to infinite.</param>
    static member StartChild(computation : Cloud<'T>, ?target : IWorkerRef, ?timeoutMilliseconds:int) : Cloud<Cloud<'T>> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let! receiver = runtime.ScheduleStartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
        return Cloud.WithAppendedStackTrace "Cloud.StartChild[T](Cloud<T> computation)" receiver
    }

    /// <summary>
    ///     Gets information on the execution cluster.
    /// </summary>
    static member CurrentWorker : Cloud<IWorkerRef> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.CurrentWorker
    }

    /// <summary>
    ///     Gets all workers in currently running cluster context.
    /// </summary>
    static member GetAvailableWorkers () : Cloud<IWorkerRef []> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return! Cloud.OfAsync <| runtime.GetAvailableWorkers()
    }

    /// <summary>
    ///     Gets total number of available workers in cluster context.
    /// </summary>
    static member GetWorkerCount () : Cloud<int> = cloud {
        let! workers = Cloud.GetAvailableWorkers()
        return workers.Length
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud process.
    /// </summary>
    static member GetProcessId () : Cloud<string> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.ProcessId
    }

    /// <summary>
    ///     Gets the assigned id of the currently running cloud task.
    /// </summary>
    static member GetTaskId () : Cloud<string> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.TaskId
    }

    /// <summary>
    ///     Gets the current scheduling context.
    /// </summary>
    static member GetSchedulingContext () = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.SchedulingContext
    }

    /// <summary>
    ///     Sets a new scheduling context for target workflow.
    /// </summary>
    /// <param name="workflow">Target workflow.</param>
    /// <param name="schedulingContext">Target scheduling context.</param>
    [<CompilerMessage("'SetSchedulingContext' only intended for runtime implementers.", 444)>]
    static member WithSchedulingContext (schedulingContext : SchedulingContext) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider>()
        let runtime' = runtime.WithSchedulingContext schedulingContext
        return! Cloud.SetResource(workflow, runtime')
    }

    /// <summary>
    ///     Force thread local execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToLocal(workflow : Cloud<'T>) = Cloud.WithSchedulingContext ThreadParallel workflow

    /// <summary>
    ///     Force sequential execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToSequential(workflow : Cloud<'T>) = Cloud.WithSchedulingContext Sequential workflow

    /// <summary>
    ///     Gets the current fault policy.
    /// </summary>
    static member GetFaultPolicy () : Cloud<FaultPolicy> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return runtime.FaultPolicy
    }

    /// <summary>
    ///     Sets a new fault policy for given workflow.
    /// </summary>
    /// <param name="policy">Updated fault policy.</param>
    /// <param name="workflow">Workflow to be used.</param>
    static member WithFaultPolicy (policy : FaultPolicy) (workflow : Cloud<'T>) : Cloud<'T> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        let runtime' = runtime.WithFaultPolicy policy
        return! Cloud.SetResource(workflow, resource)
    }
        
/// <summary>
///     Combines two cloud computations into one that executes them in parallel.
/// </summary>
/// <param name="left">The first cloud computation.</param>
/// <param name="right">The second cloud computation.</param>
let (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
    cloud { 
        let! result = 
                Cloud.Parallel<obj> [| cloud { let! value = left in return value :> obj }; 
                                        cloud { let! value = right in return value :> obj } |]
        return (result.[0] :?> 'a, result.[1] :?> 'b) 
    }

/// <summary>
///     Combines two cloud computations into one that executes them in parallel and returns the
///     result of the first computation that completes and cancels the other.
/// </summary>
/// <param name="left">The first cloud computation.</param>
/// <param name="right">The second cloud computation.</param>
let (<|>) (left : Cloud<'a>) (right : Cloud<'a>) : Cloud<'a> =
    cloud {
        let! result = 
            Cloud.Choice [| cloud { let! value = left  in return Some (value) }
                            cloud { let! value = right in return Some (value) }  |]

        return result.Value
    }

/// <summary>
///     Combines two cloud computations into one that executes them sequentially.
/// </summary>
/// <param name="left">The first cloud computation.</param>
/// <param name="right">The second cloud computation.</param>
let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }