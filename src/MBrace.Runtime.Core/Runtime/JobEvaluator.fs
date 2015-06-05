namespace MBrace.Runtime

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals

open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Runtime.Utils

/// Job evaluator abstraction
type ICloudJobEvaluator =

    /// <summary>
    ///     Asynchronously evaluates a job in the local worker.  
    /// </summary>
    /// <param name="dependencies">Local assemblies that the job depends on.</param>
    /// <param name="jobtoken">Cloud job token.</param>
    abstract Evaluate : dependencies:VagabondAssembly [] * jobtoken:ICloudJobLeaseToken -> Async<unit>

[<RequireQualifiedAccess>]
module JobEvaluator =

    /// <summary>
    ///     Asynchronously evaluates job in the local application domain.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="faultState">Job fault state.</param>
    /// <param name="job">Job instance to be executed.</param>
    let runJobAsync (manager : IRuntimeResourceManager) (faultState : (int * ExceptionDispatchInfo) option) (job : CloudJob) = async {
        let jem = new JobExecutionMonitor()
        let distributionProvider = DistributionProvider.Create(manager, job)
        let resources = resource {
            yield! manager.ResourceRegistry
            yield jem
            yield distributionProvider :> IDistributionProvider
        }

        let ctx = { Resources = resources ; CancellationToken = job.CancellationToken }

        match faultState with
        | Some(faultCount, edi) ->
            let e = edi.Reify(prepareForRaise = false)
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None ->
                let msg = sprintf "Fault exception when running job '%s', faultCount '%d'." job.JobId faultCount
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
    /// <param name="assemblies">Vagabond assemblies to be used for computation.</param>
    /// <param name="joblt">Job lease token.</param>
    let loadAndRunJobAsync (manager : IRuntimeResourceManager) (assemblies : VagabondAssembly []) (joblt : ICloudJobLeaseToken) = async {
        let logger = manager.SystemLogger
        logger.Log LogLevel.Info "Loading assemblies."
        for li in manager.AssemblyManager.LoadAssemblies assemblies do
            match li with
            | NotLoaded id -> logger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
            | LoadFault(id, e) -> logger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
            | Loaded _ -> ()

        logger.Log LogLevel.Info "Loading job."
        let! jobResult = joblt.GetJob() |> Async.Catch
        match jobResult with
        | Choice2Of2 e ->
            logger.Logf LogLevel.Error "Failed to load job '%s': %A" joblt.JobId e
            do! joblt.ParentTask.SetException (ExceptionDispatchInfo.Capture e)

        | Choice1Of2 job ->
//            if job.JobType = JobType.Root then
//                logf "Starting Root job for Process Id : %s, Name : %s" job.ProcessInfo.Id job.ProcessInfo.Name
//                do! staticConfiguration.State.ProcessManager.SetRunning(job.ProcessInfo.Id)

            logger.Logf LogLevel.Info "Starting job '%s'." job.JobId
            let sw = Stopwatch.StartNew()
            let! result = runJobAsync manager joblt.FaultState job |> Async.Catch
            sw.Stop()

            match result with
            | Choice1Of2 () -> 
                do! joblt.DeclareCompleted ()
                logger.Logf LogLevel.Info "Completed job '%s' after %O" job.JobId sw.Elapsed
            | Choice2Of2 e ->
                logger.Logf LogLevel.Error "Faulted job '%s' after %O\n%O" job.JobId sw.Elapsed e
                do! joblt.DeclareFaulted (ExceptionDispatchInfo.Capture e)
    }
       

[<AutoSerializable(false)>]
type LocalJobEvaluator(manager : IRuntimeResourceManager) =
    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            return! JobEvaluator.loadAndRunJobAsync manager assemblies jobtoken
        }

[<AutoSerializable(false)>]
type AppDomainJobEvaluator(managerF : DomainLocal<IRuntimeResourceManager>, pool : AppDomainEvaluatorPool) =

    static member Create(managerF : DomainLocal<IRuntimeResourceManager>,
                                ?initializer : unit -> unit, ?threshold : TimeSpan, 
                                ?minConcurrentDomains : int, ?maxConcurrentDomains : int) =

        let domainInitializer () = initializer |> Option.iter (fun f -> f ())
        let pool = AppDomainEvaluatorPool.Create(domainInitializer, ?threshold = threshold, 
                                                    ?minimumConcurrentDomains = minConcurrentDomains,
                                                    ?maximumConcurrentDomains = maxConcurrentDomains)

        new AppDomainJobEvaluator(managerF, pool)

    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            // avoid capturing evaluator in closure
            let managerF = managerF
            let eval () = async { 
                let manager = managerF.Value 
                return! JobEvaluator.loadAndRunJobAsync manager assemblies jobtoken 
            }

            return! pool.EvaluateAsync(jobtoken.Dependencies, eval ())
        }

    interface IDisposable with
        member __.Dispose () = (pool :> IDisposable).Dispose()