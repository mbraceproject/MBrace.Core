namespace MBrace.Runtime.InMemoryRuntime

open System
open System.Threading
open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils

#nowarn "444"

/// .NET ThreadPool distribution provider
[<Sealed; AutoSerializable(false)>]
type ThreadPoolDistributionProvider private (faultPolicy : FaultPolicy, memoryMode : MemoryEmulation, logger : ICloudLogger) =

    static let mkNestedCts (ct : ICloudCancellationToken) = 
        InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource [| ct |] :> ICloudCancellationTokenSource

    /// <summary>
    ///     Creates a new threadpool runtime instance.
    /// </summary>
    /// <param name="logger">Logger for runtime. Defaults to no logging.</param>
    /// <param name="faultPolicy">Fault policy for runtime. Defaults to no retry.</param>
    /// <param name="memoryMode">Memory semantics used for parallelism. Defaults to shared memory.</param>
    static member Create (?logger : ICloudLogger, ?faultPolicy, ?memoryMode : MemoryEmulation) = 
        let logger = 
            match logger with 
            | Some l -> l 
            | None -> { new ICloudLogger with member __.Log _ = () }

        let faultPolicy = match faultPolicy with Some f -> f | None -> FaultPolicy.NoRetry
        let memoryMode = defaultArg memoryMode MemoryEmulation.Shared
        new ThreadPoolDistributionProvider(faultPolicy, memoryMode, logger)
        
    interface IDistributionProvider with
        member __.CreateLinkedCancellationTokenSource (parents : ICloudCancellationToken[]) = async {
            return InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource parents :> _
        }

        member __.ProcessId = sprintf "In-Memory cloud process (pid:%d)" <| System.Diagnostics.Process.GetCurrentProcess().Id
        member __.JobId = sprintf "TheadId %d" <| System.Threading.Thread.CurrentThread.ManagedThreadId
        member __.Logger = logger
        member __.IsTargetedWorkerSupported = false
        member __.GetAvailableWorkers () = async {
            return [| InMemoryWorker.LocalInstance :> IWorkerRef |]
        }

        member __.CurrentWorker = InMemoryWorker.LocalInstance :> IWorkerRef

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newFp = new ThreadPoolDistributionProvider(newFp, memoryMode, logger) :> IDistributionProvider

        member __.IsForcedLocalParallelismEnabled = true
        member __.WithForcedLocalParallelismSetting setting =
            if setting then new ThreadPoolDistributionProvider(faultPolicy, MemoryEmulation.Shared, logger) :> IDistributionProvider
            else
                __ :> IDistributionProvider

        member __.ScheduleParallel computations = cloud {
            return! ThreadPool.Parallel(mkNestedCts, memoryMode, Seq.map fst computations)
        }

        member __.ScheduleChoice computations = cloud {
            return! ThreadPool.Choice(mkNestedCts, memoryMode, Seq.map fst computations)
        }

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts, MemoryEmulation.Shared, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts, MemoryEmulation.Shared, computations)

        member __.ScheduleStartAsTask (workflow:Cloud<'T>, faultPolicy:FaultPolicy, ?cancellationToken:ICloudCancellationToken, ?target:IWorkerRef, ?taskName:string) = cloud {
            ignore taskName
            target |> Option.iter (fun _ -> raise <| new System.NotSupportedException("Targeted workers not supported in In-Memory runtime."))
            return! ThreadPool.StartAsTask(workflow, memoryMode, faultPolicy = faultPolicy, ?cancellationToken = cancellationToken)
        }