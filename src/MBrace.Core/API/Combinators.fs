namespace Nessos.MBrace

#nowarn "444"

open System.Threading
open System.Threading.Tasks

open Nessos.MBrace.Runtime

/// Cloud workflows static methods
type Cloud =

    /// <summary>
    ///     Gets the current cancellation token.
    /// </summary>
    static member CancellationToken = 
        Cloud.FromContinuations(fun ctx -> ctx.scont ctx.CancellationToken)

    /// <summary>
    ///     Raise an exception.
    /// </summary>
    /// <param name="e">exception to be raised.</param>
    static member Raise<'T> (e : exn) : Cloud<'T> = raiseM e

    /// <summary>
    ///     Catch exception from given cloud workflow.
    /// </summary>
    /// <param name="cloudWorkflow"></param>
    static member Catch(cloudWorkflow : Cloud<'T>) : Cloud<Choice<'T, exn>> = cloud {
        try
            let! res = cloudWorkflow
            return Choice1Of2 res
        with e ->
            return Choice2Of2 e
    }

    /// <summary>
    ///     Creates a cloud workflow that asynchronously sleeps for a given amount of time.
    /// </summary>
    /// <param name="timeoutMilliseconds"></param>
    static member Sleep(timeoutMilliseconds : int) : Cloud<unit> = 
        Cloud.OfAsync<unit>(Async.Sleep timeoutMilliseconds)

    /// <summary>
    ///     Wraps an asynchronous workflow into a cloud workflow.
    /// </summary>
    /// <param name="asyncWorkflow">Asynchronous workflow to be wrapped.</param>
    static member OfAsync<'T>(asyncWorkflow : Async<'T>) : Cloud<'T> = 
        Cloud.FromContinuations(fun ctx -> Async.StartWithContinuations(asyncWorkflow, ctx.scont, ctx.econt, ctx.ccont, ctx.CancellationToken))

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Log(logEntry : string) : Cloud<unit> = cloud {
        let! logger = Cloud.TryGetResource<ILoggingProvider> ()
        return 
            match logger with
            | None -> ()
            | Some l -> l.Log logEntry
    }

    /// <summary>
    ///     Writes an entry to a logging provider, if it exists.
    /// </summary>
    /// <param name="logEntry">Added log entry.</param>
    static member Logf fmt : Cloud<unit> = Printf.ksprintf Cloud.Log fmt

    /// <summary>
    ///     Performs a cloud computations, discarding its result
    /// </summary>
    /// <param name="workflow"></param>
    static member Ignore (workflow : Cloud<'T>) : Cloud<unit> = cloud { let! _ = workflow in return () }

    /// <summary>
    ///     Asynchronously await task completion
    /// </summary>
    /// <param name="task">Task to be awaited</param>
    static member AwaitTask (task : Task<'T>) : Cloud<'T> = 
        Cloud.OfAsync(Async.AwaitTask task)

    /// <summary>
    ///     Cloud.Parallel combinator
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Parallel (computations : seq<Cloud<'T>>) : Cloud<'T []> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return! runtime.ScheduleParallel computations
    }

    /// <summary>
    ///     Cloud.Choice combinator
    /// </summary>
    /// <param name="computations">Input computations to be executed in parallel.</param>
    static member Choice (computations : seq<Cloud<'T option>>) : Cloud<'T option> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return! runtime.ScheduleChoice computations
    }

    /// <summary>
    ///     Start cloud computation as child. Returns a cloud workflow that queries the result.
    /// </summary>
    /// <param name="computation">Computation to be executed.</param>
    /// <param name="target">Optional worker to execute the computation on; defaults to scheduler decision.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds; defaults to infinite.</param>
    static member StartChild(computation : Cloud<'T>, ?target : IWorkerRef, ?timeoutMilliseconds:int) : Cloud<Cloud<'T>> = cloud {
        let! runtime = Cloud.GetResource<IRuntimeProvider> ()
        return! runtime.ScheduleStartChild(computation, ?target = target, ?timeoutMilliseconds = timeoutMilliseconds)
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
    ///     Force thread local execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToLocal(workflow : Cloud<'T>) = Cloud.SetSchedulingContext(workflow, ThreadParallel)

    /// <summary>
    ///     Force sequential execution semantics for given cloud workflow.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    static member ToSequential(workflow : Cloud<'T>) = Cloud.SetSchedulingContext(workflow, Sequential)


/// Cloud reference methods
type CloudRef =

    /// Allocate a new Cloud reference
    static member New(value : 'T) : Cloud<ICloudRef<'T>> = cloud {
        let! storageProvider = Cloud.GetResource<IStorageProvider> ()
        return! storageProvider.CreateCloudRef value |> Cloud.OfAsync
    }

    /// Dereference a Cloud reference.
    static member Dereference(cloudRef : ICloudRef<'T>) : Cloud<'T> = Cloud.OfAsync <| cloudRef.GetValue()


/// [omit]
/// Contains common operators for cloud computation.

[<AutoOpen>]
module Operators =
        
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