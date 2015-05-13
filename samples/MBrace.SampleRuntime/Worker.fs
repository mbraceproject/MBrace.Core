module internal MBrace.SampleRuntime.Worker

open System
open System.Diagnostics
open System.Threading

open Nessos.Vagabond.AppDomainPool

open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond

open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Types
open MBrace.SampleRuntime.RuntimeProvider

type IJobEvaluator =
    abstract Id : string
    abstract Evaluate : state:RuntimeState * logger:ICloudLogger * pjob:PickledJob * leaseMonitor:LeaseMonitor * faultCount:int -> Async<unit>


/// <summary>
///     Initializes a worker loop. Worker polls job queue of supplied
///     runtime for available jobs and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentJobs">Maximum jobs to be executed concurrently by worker.</param>
let initWorker (runtime : RuntimeState) (logger : ICloudLogger) (maxConcurrentJobs : int) (evaluator : IJobEvaluator) = async {

    let localEndPoint = MBrace.SampleRuntime.Config.LocalEndPoint
    logger.Logf "MBrace worker (%s) initialized on %O." evaluator.Id localEndPoint
    logger.Logf "Listening to job queue at %O." runtime.IPEndPoint

    let currentJobCount = ref 0

    let rec loop () = async {
        if !currentJobCount >= maxConcurrentJobs then
            do! Async.Sleep 500
            return! loop ()
        else
            try
                let! result = runtime.JobQueue.TryDequeue Worker.LocalWorker
                match result with
                | None -> do! Async.Sleep 500
                | Some (pjob, faultCount, leaseMonitor) ->
                    let _ = Interlocked.Increment currentJobCount
                    let! _ =  Async.StartChild <| async {
                        try do! evaluator.Evaluate(runtime, logger, pjob, leaseMonitor, faultCount)
                        finally let _ = Interlocked.Decrement currentJobCount in ()
                    }

                    do! Async.Sleep 200
            with e -> 
                printfn "WORKER FAULT: %O" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}


type LocalJobEvaluator(?showAppDomain : bool) =
    let appDomainPrefix =
        if defaultArg showAppDomain false then
            sprintf "AppDomain[%s]: " System.AppDomain.CurrentDomain.FriendlyName
        else
            ""

    interface IJobEvaluator with
        member __.Id = "InDomain"
        member __.Evaluate(runtime : RuntimeState, logger : ICloudLogger, pjob : PickledJob, leaseMonitor : LeaseMonitor, faultCount : int) = async {
            logger.Logf "%sStarting job %s of type '%s'." appDomainPrefix pjob.JobId pjob.TypeName

            use! hb = leaseMonitor.InitHeartBeat()
            let sw = Stopwatch.StartNew()

            let! fault = 
                async {
                    let! job = runtime.UnPickle pjob
                    let runtimeP = DistributionProvider.FromJob runtime pjob.Dependencies job
                    do! Job.RunAsync runtimeP faultCount job
                } |> Async.Catch

            do sw.Stop()

            match fault with
            | Choice1Of2 () -> 
                leaseMonitor.Release()
                logger.Logf "%sJob %s completed after %O." appDomainPrefix pjob.JobId sw.Elapsed
                                
            | Choice2Of2 e -> 
                leaseMonitor.DeclareFault()
                logger.Logf "%sJob %s faulted with:\n %O." appDomainPrefix pjob.JobId e
        }

type AppDomainJobEvaluator() =

    static let mkAppDomainInitializer () =
        let wd = Config.WorkingDirectory
        fun () -> Config.Init(wd, cleanup = false)

    let pool = AppDomainEvaluatorPool.Create(mkAppDomainInitializer(), threshold = TimeSpan.FromHours 2.)

    interface IJobEvaluator with
        member __.Id = "AppDomain isolated"
        member __.Evaluate(runtime : RuntimeState, logger : ICloudLogger, pjob : PickledJob, leaseMonitor : LeaseMonitor, faultCount : int) = async {
            let eval() = async {
                let local = new LocalJobEvaluator(showAppDomain = true) :> IJobEvaluator
                return! local.Evaluate(runtime, logger, pjob, leaseMonitor, faultCount)
            }

            return! pool.EvaluateAsync(pjob.Dependencies, eval ())
        }



open Nessos.Thespian

let workerManager (logger : ICloudLogger) (evaluator : IJobEvaluator) (cts: CancellationTokenSource) (msg: WorkerManager) =
    async {
        match msg with
        | SubscribeToRuntime(rc, runtimeStateStr, maxConcurrentJobs) ->
            let runtimeState =
                let bytes = System.Convert.FromBase64String(runtimeStateStr)

                Config.Pickler.UnPickle<RuntimeState> bytes

            Async.Start(initWorker runtimeState logger maxConcurrentJobs evaluator, cts.Token)
            try do! rc.Reply() with e -> logger.Logf "Failed to confirm worker subscription to client: %O" e
            return cts
        | Unsubscribe rc ->
            cts.Cancel()
            try do! rc.Reply() with e -> logger.Logf "Failed to confirm worker unsubscription to client: %O" e
            return new CancellationTokenSource()
    }