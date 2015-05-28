namespace MBrace.Runtime

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals

open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond

/// Job evaluator abstraction
type ICloudJobEvaluator =
    /// Asynchronously evaluates a job in the local worker.
    abstract Evaluate : dependencies:VagabondAssembly [] * jobtoken:ICloudJobLeaseToken -> Async<unit>

[<RequireQualifiedAccess>]
module JobEvaluator =

//    let donwloadDependencies (assemblyManager : StoreAssemblyManager) (jobtoken : ICloudJobLeaseToken) = async {
//        let! assemblies = assemblyManager.DownloadDependencies(jobtoken.Dependencies)
//        return List.toArray assemblies
//    }

    /// <summary>
    ///     Asynchronously evaluates job in the local application domain.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="faultState">Job fault state.</param>
    /// <param name="job">Job instance to be executed.</param>
    let runJobAsync (manager : IRuntimeResourceManager) (faultState : (int * exn) option) (job : CloudJob) = async {
        let jem = new JobExecutionMonitor()
        let distributionProvider = DistributionProvider.Create(manager, job)
        let resources = resource {
            yield! manager.ResourceRegistry
            yield jem
            yield distributionProvider :> IDistributionProvider
        }

        let ctx = { Resources = resources ; CancellationToken = job.CancellationToken }

        match faultState with
        | Some(faultCount, e) ->
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None ->
                let msg = sprintf "Fault exception when running job '%s', faultCount '%d'" job.JobId faultCount
                let faultException = new FaultException(msg, e)
                job.Econt ctx (ExceptionDispatchInfo.Capture faultException)

            | Some retryTimeout ->
                do! Async.Sleep (int retryTimeout.TotalMilliseconds)
                do job.StartJob ctx

        | None -> do job.StartJob ctx

        return! JobExecutionMonitor.AwaitCompletion jem
    }
    
    /// <summary>
    ///     loads job to local application domain and evaluates it locally.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="sysLogger">System logger for evaluator.</param>
    /// <param name="assemblies">Vagabond assemblies to be used for computation.</param>
    /// <param name="joblt">Job lease token.</param>
    let loadAndRunJobAsync (manager : IRuntimeResourceManager) (sysLogger : ICloudLogger) 
                            (assemblies : VagabondAssembly []) (joblt : ICloudJobLeaseToken) = async {

        sysLogger.Log "Loading assemblies."
        let loadInfo = VagabondRegistry.Instance.LoadVagabondAssemblies assemblies
        loadInfo |> List.iter (function NotLoaded id -> sysLogger.Logf "could not load assembly '%s'" id.FullName 
                                        | LoadFault(id, e) -> sysLogger.Logf "error loading assembly '%s':\n%O" id.FullName e
                                        | Loaded _ -> ())

        sysLogger.Log "Loading job."
        let! jobResult = joblt.GetJob() |> Async.Catch
        match jobResult with
        | Choice2Of2 e ->
            sysLogger.Logf "Failed to load job '%s': %A" joblt.JobId e
            do! joblt.ParentTask.SetException (ExceptionDispatchInfo.Capture e)

        | Choice1Of2 job ->
//            if job.JobType = JobType.Root then
//                logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
//                do! staticConfiguration.State.ProcessManager.SetRunning(job.ProcessInfo.Id)

            sysLogger.Logf "Starting job '%s'." job.JobId
            let sw = Stopwatch.StartNew()
            let! result = runJobAsync manager joblt.FaultState job |> Async.Catch
            sw.Stop()

            match result with
            | Choice1Of2 () -> 
                do! joblt.DeclareCompleted ()
                sysLogger.Logf "Completed job\n%O\nTime : %O" job sw.Elapsed
            | Choice2Of2 e ->
                sysLogger.Logf "Faulted job\n%O\nTime : %O" job sw.Elapsed
                do! joblt.DeclareFaulted (ExceptionDispatchInfo.Capture e)
    }
       

type LocalJobEvaluator(manager : IRuntimeResourceManager, sysLogger : ICloudLogger) =
    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            return! JobEvaluator.loadAndRunJobAsync manager sysLogger assemblies jobtoken
        }


[<NoEquality; NoComparison>]
type AppDomainConfig =
    {
        ResourceManager : IRuntimeResourceManager
        SystemLogger    : ICloudLogger
    }

type AppDomainJobEvaluator(config : DomainLocal<AppDomainConfig>, ?threshold : TimeSpan, 
                                ?minConcurrentDomains : int, ?maxConcurrentDomains : int, ?initializer : unit -> unit) =

    let domainInitializer () = initializer |> Option.iter (fun f -> f ()) ; ignore config.Value
    let pool = AppDomainEvaluatorPool.Create(domainInitializer, ?threshold = threshold, 
                                                ?minimumConcurrentDomains = minConcurrentDomains,
                                                ?maximumConcurrentDomains = maxConcurrentDomains)

    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            let eval () = async { 
                let config = config.Value 
                return! JobEvaluator.loadAndRunJobAsync config.ResourceManager config.SystemLogger assemblies jobtoken 
            }

            return! pool.EvaluateAsync(jobtoken.Dependencies, eval ())
        }

    interface IDisposable with
        member __.Dispose () = (pool :> IDisposable).Dispose()