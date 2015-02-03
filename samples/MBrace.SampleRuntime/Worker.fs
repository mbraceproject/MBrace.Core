module internal MBrace.SampleRuntime.Worker

open System
open System.Diagnostics
open System.Threading

open Nessos.Vagabond.AppDomainPool

open MBrace.Runtime
open MBrace.Runtime.Vagabond

open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Tasks
open MBrace.SampleRuntime.RuntimeProvider

type IWorkerLogger =
    abstract Log : string -> unit

type ITaskEvaluator =
    abstract Id : string
    abstract Evaluate : state:RuntimeState * logger:IWorkerLogger * ptask:PickledTask * leaseMonitor:LeaseMonitor * faultCount:int -> Async<unit>

type ConsoleTaskLogger () =
    interface IWorkerLogger with
        member __.Log msg = System.Console.WriteLine msg

[<AutoOpen>]
module LogUtils =
    type IWorkerLogger with
        member l.Logf fmt = Printf.ksprintf l.Log fmt

/// <summary>
///     Initializes a worker loop. Worker polls task queue of supplied
///     runtime for available tasks and executes as appropriate.
/// </summary>
/// <param name="runtime">Runtime to subscribe to.</param>
/// <param name="maxConcurrentTasks">Maximum tasks to be executed concurrently by worker.</param>
let initWorker (runtime : RuntimeState) (logger : IWorkerLogger) (maxConcurrentTasks : int) (evaluator : ITaskEvaluator) = async {

    let localEndPoint = MBrace.SampleRuntime.Config.LocalEndPoint
    logger.Logf "MBrace worker (%s) initialized on %O." evaluator.Id localEndPoint
    logger.Logf "Listening to task queue at %O." runtime.IPEndPoint

    let currentTaskCount = ref 0

    let rec loop () = async {
        if !currentTaskCount >= maxConcurrentTasks then
            do! Async.Sleep 500
            return! loop ()
        else
            try
                let! task = runtime.TaskQueue.TryDequeue Worker.LocalWorker
                match task with
                | None -> do! Async.Sleep 500
                | Some (ptask, faultCount, leaseMonitor) ->
                    let _ = Interlocked.Increment currentTaskCount
                    let! _ =  Async.StartChild <| async {
                        try do! evaluator.Evaluate(runtime, logger, ptask, leaseMonitor, faultCount)
                        finally let _ = Interlocked.Decrement currentTaskCount in ()
                    }

                    do! Async.Sleep 200
            with e -> 
                printfn "WORKER FAULT: %O" e
                do! Async.Sleep 1000

            return! loop ()
    }

    return! loop ()
}


type LocalTaskEvaluator(?showAppDomain : bool) =
    let appDomainPrefix =
        if defaultArg showAppDomain false then
            sprintf "AppDomain[%s]: " System.AppDomain.CurrentDomain.FriendlyName
        else
            ""

    interface ITaskEvaluator with
        member __.Id = "InDomain"
        member __.Evaluate(runtime : RuntimeState, logger : IWorkerLogger, ptask : PickledTask, leaseMonitor : LeaseMonitor, faultCount : int) = async {
            logger.Logf "%sStarting task %s of type '%s'." appDomainPrefix ptask.TaskId ptask.TypeName

            use! hb = leaseMonitor.InitHeartBeat()
            let sw = Stopwatch.StartNew()

            let! fault = 
                async {
                    let! task = runtime.UnPickle ptask
                    let runtimeP = RuntimeProvider.FromTask runtime ptask.Dependencies task
                    do! Task.RunAsync runtimeP faultCount task
                } |> Async.Catch

            do sw.Stop()

            match fault with
            | Choice1Of2 () -> 
                leaseMonitor.Release()
                printfn "%sTask %s completed after %O." appDomainPrefix ptask.TaskId sw.Elapsed
                                
            | Choice2Of2 e -> 
                leaseMonitor.DeclareFault()
                printfn "%sTask %s faulted with:\n %O." appDomainPrefix ptask.TaskId e
        }

type AppDomainTaskEvaluator() =

    static let mkAppDomainInitializer () =
        let wd = Config.WorkingDirectory
        fun () -> Config.Init(wd, cleanup = false)

    let pool = AppDomainEvaluatorPool.Create(mkAppDomainInitializer(), threshold = TimeSpan.FromHours 2.)

    interface ITaskEvaluator with
        member __.Id = "AppDomain isolated"
        member __.Evaluate(runtime : RuntimeState, logger : IWorkerLogger, ptask : PickledTask, leaseMonitor : LeaseMonitor, faultCount : int) = async {
            let eval() = async {
                let local = new LocalTaskEvaluator(showAppDomain = true) :> ITaskEvaluator
                return! local.Evaluate(runtime, logger, ptask, leaseMonitor, faultCount)
            }

            return! pool.EvaluateAsync(ptask.Dependencies, eval ())
        }



open Nessos.Thespian

let workerManager (logger : IWorkerLogger) (evaluator : ITaskEvaluator) (cts: CancellationTokenSource) (msg: WorkerManager) =
    async {
        match msg with
        | SubscribeToRuntime(rc, runtimeStateStr, maxConcurrentTasks) ->
            let runtimeState =
                let bytes = System.Convert.FromBase64String(runtimeStateStr)

                Config.Pickler.UnPickle<RuntimeState> bytes

            Async.Start(initWorker runtimeState logger maxConcurrentTasks evaluator, cts.Token)
            try do! rc.Reply() with e -> printfn "Failed to confirm worker subscription to client: %O" e
            return cts
        | Unsubscribe rc ->
            cts.Cancel()
            try do! rc.Reply() with e -> printfn "Failed to confirm worker unsubscription to client: %O" e
            return new CancellationTokenSource()
    }