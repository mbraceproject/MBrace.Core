namespace MBrace.Runtime.InMemory

open System
open System.Threading
open System.Threading.Tasks

open MBrace
open MBrace.Runtime
open MBrace.Continuation

#nowarn "444"

[<AutoSerializable(false)>]
type internal InMemoryCancellationToken (token : CancellationToken) =
    member __.Token = token
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested

[<AutoSerializable(false)>]
type internal InMemoryCancellationTokenSource (cts : CancellationTokenSource) =
    interface ICloudCancellationTokenSource with
        member __.Cancel() = cts.Cancel()
        member __.Token = new InMemoryCancellationToken(cts.Token) :> _

    static member CreateLinkedCancellationTokenSource(tokens : seq<ICloudCancellationToken>) =
        let cts =
            tokens
            |> Seq.map (fun t -> (t :?> InMemoryCancellationToken).Token)
            |> Seq.toArray
            |> CancellationTokenSource.CreateLinkedTokenSource

        new InMemoryCancellationTokenSource(cts)

[<AutoSerializable(false)>]
type internal InMemoryCloudTask<'T> (task : Task<'T>) =
    interface ICloudTask<'T> with
        member __.Id = sprintf ".NET task %d" task.Id
        member __.AwaitResult(?timeoutMilliseconds:int) = Cloud.AwaitTask(task, ?timeoutMilliseconds = timeoutMilliseconds)
        member __.TryGetResult () = cloud { return task.TryGetResult() }
        member __.Status = cloud { return task.Status }
        member __.IsCompleted = cloud { return task.IsCompleted }
        member __.IsFaulted = cloud { return task.IsFaulted }
        member __.IsCanceled = cloud { return task.IsCanceled }

/// .NET ThreadPool runtime provider
[<Sealed; AutoSerializable(false)>]
type ThreadPoolRuntime private (cancellationToken : InMemoryCancellationToken, context : SchedulingContext, faultPolicy : FaultPolicy, logger : ICloudLogger) =

    let taskId = System.Guid.NewGuid().ToString()

    /// <summary>
    ///     Creates a new threadpool runtime instance.
    /// </summary>
    /// <param name="logger">Logger for runtime. Defaults to no logging.</param>
    /// <param name="faultPolicy">Fault policy for runtime. Defaults to no retry.</param>
    static member Create (cancellationToken, ?logger : ICloudLogger, ?faultPolicy) = 
        let logger = 
            match logger with 
            | Some l -> l 
            | None -> { new ICloudLogger with member __.Log _ = () }

        let faultPolicy = match faultPolicy with Some f -> f | None -> FaultPolicy.NoRetry
        new ThreadPoolRuntime(cancellationToken, ThreadParallel, faultPolicy, logger)
        
    interface ICloudRuntimeProvider with
        member __.CancellationToken = cancellationToken :> _
        member __.CreateLinkedCancellationTokenSource (parents : seq<ICloudCancellationToken>) = async {
            return InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource parents :> _
        }

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

            new ThreadPoolRuntime(cancellationToken, newContext, faultPolicy, logger) :> ICloudRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newFp = new ThreadPoolRuntime(cancellationToken, context, newFp, logger) :> ICloudRuntimeProvider

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

        member __.ScheduleStartAsTask (workflow:Cloud<'T>, cancellationToken:ICloudCancellationToken, ?target:IWorkerRef) = cloud {
            match context with
            | Sequential ->
                let cancellationToken = (cancellationToken :?> InMemoryCancellationToken).Token
                let! result = Cloud.Catch workflow

//        member __.ScheduleStartChild (workflow, ?target:IWorkerRef, ?timeoutMilliseconds:int) = 
//            match context with
//            | Sequential -> Sequential.StartChild workflow
//            | _ -> ThreadPool.StartChild(workflow, ?timeoutMilliseconds = timeoutMilliseconds)