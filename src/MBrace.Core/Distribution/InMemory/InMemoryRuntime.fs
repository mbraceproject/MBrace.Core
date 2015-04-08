﻿namespace MBrace.Runtime.InMemory

open System
open System.Threading
open System.Threading.Tasks

open MBrace.Core
open MBrace.Runtime
open MBrace.Continuation

#nowarn "444"

/// Cloud task implementation that wraps around System.Threading.Task for inmemory runtimes
[<AutoSerializable(false)>]
type InMemoryTask<'T> internal (task : Task<'T>) =
    interface ICloudTask<'T> with
        member __.Id = sprintf ".NET task %d" task.Id
        member __.AwaitResult(?timeoutMilliseconds:int) = Cloud.AwaitTask(task, ?timeoutMilliseconds = timeoutMilliseconds)
        member __.TryGetResult () = local { return task.TryGetResult() }
        member __.Status = task.Status
        member __.IsCompleted = task.IsCompleted
        member __.IsFaulted = task.IsFaulted
        member __.IsCanceled = task.IsCanceled
        member __.Result = task.GetResult()

/// Cloud cancellation token implementation that wraps around System.Threading.CancellationToken
[<AutoSerializable(false)>]
type InMemoryCancellationToken (token : CancellationToken) =
    new () = new InMemoryCancellationToken(new CancellationToken())
    /// Local System.Threading.CancellationToken instance
    member __.LocalToken = token
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested
        member __.LocalToken = token

/// Cloud cancellation token source implementation that wraps around System.Threading.CancellationTokenSource
[<AutoSerializable(false)>]
type InMemoryCancellationTokenSource (cts : CancellationTokenSource) =
    let token = new InMemoryCancellationToken(cts.Token)
    new () = new InMemoryCancellationTokenSource(new CancellationTokenSource())
    /// InMemoryCancellationToken instance
    member __.Token = token
    /// Trigger cancelation for the cts
    member __.Cancel() = cts.Cancel()
    /// Local System.Threading.CancellationTokenSource instance
    member __.LocalCancellationTokenSource = cts
    interface ICloudCancellationTokenSource with
        member __.Cancel() = cts.Cancel()
        member __.Token = token :> _

    /// <summary>
    ///     Creates a local linked cancellation token source from provided parent tokens
    /// </summary>
    /// <param name="parents">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken []) =
        let ltokens = parents |> Array.map (fun t -> t.LocalToken)
        let lcts =
            if Array.isEmpty ltokens then new CancellationTokenSource()
            else
                CancellationTokenSource.CreateLinkedTokenSource ltokens

        new InMemoryCancellationTokenSource(lcts)

[<Sealed; AutoSerializable(false)>]
type internal InMemoryWorker private () =
    static let singleton = new InMemoryWorker()
    let name = System.Net.Dns.GetHostName()
    interface IWorkerRef with
        member __.Type = "InMemory worker"
        member __.Id = name
        member __.ProcessorCount = Environment.ProcessorCount
        member __.CompareTo(other : obj) =
            match other with
            | :? InMemoryWorker -> 0
            | :? IWorkerRef as wr -> compare name wr.Id
            | _ -> invalidArg "other" "invalid comparand."

    static member Instance = singleton

/// .NET ThreadPool distribution provider
[<Sealed; AutoSerializable(false)>]
type ThreadPoolRuntime private (faultPolicy : FaultPolicy, logger : ICloudLogger) =

    static let mkNestedCts (ct : ICloudCancellationToken) = 
        InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource [| ct |] :> ICloudCancellationTokenSource

    /// <summary>
    ///     Creates a new threadpool runtime instance.
    /// </summary>
    /// <param name="logger">Logger for runtime. Defaults to no logging.</param>
    /// <param name="faultPolicy">Fault policy for runtime. Defaults to no retry.</param>
    static member Create (?logger : ICloudLogger, ?faultPolicy) = 
        let logger = 
            match logger with 
            | Some l -> l 
            | None -> { new ICloudLogger with member __.Log _ = () }

        let faultPolicy = match faultPolicy with Some f -> f | None -> FaultPolicy.NoRetry
        new ThreadPoolRuntime(faultPolicy, logger)
        
    interface IDistributionProvider with
        member __.CreateLinkedCancellationTokenSource (parents : ICloudCancellationToken[]) = async {
            return InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource parents :> _
        }

        member __.ProcessId = sprintf "In-Memory cloud process (pid:%d)" <| System.Diagnostics.Process.GetCurrentProcess().Id
        member __.JobId = sprintf "TheadId %d" <| System.Threading.Thread.CurrentThread.ManagedThreadId
        member __.Logger = logger
        member __.IsTargetedWorkerSupported = false
        member __.GetAvailableWorkers () = async {
            return [| InMemoryWorker.Instance :> IWorkerRef |]
        }

        member __.CurrentWorker = InMemoryWorker.Instance :> IWorkerRef

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newFp = new ThreadPoolRuntime(newFp, logger) :> IDistributionProvider

        member __.IsForcedLocalParallelismEnabled = true
        member __.WithForcedLocalParallelismSetting _ = __ :> _

        member __.ScheduleParallel computations = cloud {
            return! ThreadPool.Parallel(mkNestedCts, Seq.map fst computations)
        }

        member __.ScheduleChoice computations = cloud {
            return! ThreadPool.Choice(mkNestedCts, Seq.map fst computations)
        }

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts, computations)

        member __.ScheduleStartAsTask (workflow:Cloud<'T>, faultPolicy:FaultPolicy, ?cancellationToken:ICloudCancellationToken, ?target:IWorkerRef) = cloud {
            target |> Option.iter (fun _ -> raise <| new System.NotSupportedException("Targeted workers not supported in In-Memory runtime."))
            let! resources = Cloud.GetResourceRegistry()
            let cancellationToken = match cancellationToken with Some ct -> ct | None -> new InMemoryCancellationToken() :> _
            let runtimeP = new ThreadPoolRuntime(faultPolicy, logger) 
            let resources' = resources.Register (runtimeP :> IDistributionProvider)
            let task = Cloud.StartAsTask(workflow, resources', cancellationToken)
            return new InMemoryTask<'T>(task) :> _
        }