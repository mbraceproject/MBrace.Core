namespace MBrace.Runtime

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Runtime.Utils

/// Job evaluator abstraction
type ICloudJobEvaluator =
    inherit IDisposable

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
    /// <param name="currentWorker">Current worker executing job.</param>
    /// <param name="faultState">Job fault state.</param>
    /// <param name="job">Job instance to be executed.</param>
    let runJobAsync (manager : IRuntimeManager) (currentWorker : IWorkerId) 
                    (faultState : CloudJobFaultInfo) (job : CloudJob) = async {

        let logger = manager.SystemLogger
        let jem = new JobExecutionMonitor()
        use! distributionProvider = ParallelismProvider.Create(currentWorker, manager, job)
        let resources = resource {
            yield! manager.ResourceRegistry
            match job.TaskEntry.Info.AdditionalResources with Some r -> yield! r | None -> ()
            yield jem
            yield currentWorker
            yield manager
            yield distributionProvider :> IParallelismProvider
        }

        let ctx = { Resources = resources ; CancellationToken = job.CancellationToken }

        match faultState with
        | IsTargetedJobOfDeadWorker (_,w) ->
            // always throw a fault exception if dead worker
            logger.Logf LogLevel.Info "Job '%O' originally assigned to dead worker '%s'." job.Id w.Id
            let worker = Option.get job.TargetWorker // this is assumed to be 'Some' here
            let e = new FaultException(sprintf "Could not communicate with target worker '%O'." worker)
            job.Econt ctx (ExceptionDispatchInfo.Capture e)

        | FaultDeclaredByWorker(faultCount, latestError, w) ->
            logger.Logf LogLevel.Info "Job '%O' faulted %d times while executed in worker '%O'." job.Id faultCount w
            // consult user-supplied fault policy to decide on further action
            let e = latestError.Reify(prepareForRaise = false)
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None ->
                let msg = sprintf "Job '%O' given up after it faulted %d times." job.Id faultCount
                let faultException = new FaultException(msg, e)
                job.Econt ctx (ExceptionDispatchInfo.Capture faultException)

            | Some retryTimeout ->
                do! Async.Sleep (int retryTimeout.TotalMilliseconds)
                do job.StartJob ctx

        | WorkerDeathWhileProcessingJob (faultCount, latestWorker) ->
            logger.Logf LogLevel.Info "Job '%O' faulted %d times while being processed by nonresponsive worker '%O'." job.Id faultCount latestWorker
            // consult user-supplied fault policy to decide on further action
            let msg = sprintf "Job '%O' was being processed by worker '%O' which has died." job.Id latestWorker
            let e = new FaultException(msg) :> exn
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None -> job.Econt ctx (ExceptionDispatchInfo.Capture e)
            | Some retryTimeout ->
                do! Async.Sleep (int retryTimeout.TotalMilliseconds)
                do job.StartJob ctx

        | NoFault ->  
            // no faults, proceed normally  
            do job.StartJob ctx

        return! JobExecutionMonitor.AwaitCompletion jem
    }
    
    /// <summary>
    ///     loads job to local application domain and evaluates it locally.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="currentWorker">Current worker executing job.</param>
    /// <param name="assemblies">Vagabond assemblies to be used for computation.</param>
    /// <param name="joblt">Job lease token.</param>
    let loadAndRunJobAsync (manager : IRuntimeManager) (currentWorker : IWorkerId) 
                            (assemblies : VagabondAssembly []) (joblt : ICloudJobLeaseToken) = async {

        let logger = manager.SystemLogger
        logger.Logf LogLevel.Debug "Loading assembly dependencies for job '%O'." joblt.Id
        for li in manager.AssemblyManager.LoadAssemblies assemblies do
            match li with
            | NotLoaded id -> logger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
            | LoadFault(id, e) -> logger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
            | Loaded _ -> ()

        logger.Logf LogLevel.Debug "Deserializing job '%O'." joblt.Id
        let! jobResult = joblt.GetJob() |> Async.Catch
        match jobResult with
        | Choice2Of2 e ->
            // failure to deserialize job triggers special error handling;
            // trigger root task as faulted without consulting fault policy.
            logger.Logf LogLevel.Error "Failed to deserialize job '%O':\n%O" joblt.Id e
            let e = new FaultException(sprintf "Failed to deserialize job '%O'." joblt.Id, e)
            let edi = ExceptionDispatchInfo.Capture e
            do! joblt.TaskEntry.DeclareStatus Faulted
            let! _ = joblt.TaskEntry.TrySetResult(TaskResult.Exception edi, currentWorker)
            do! joblt.DeclareCompleted()

        | Choice1Of2 job ->
            if job.JobType = CloudJobType.TaskRoot then
                match job.TaskEntry.Info.Name with
                | None -> logger.Logf LogLevel.Info "Starting cloud task '%s' of type '%s'." job.TaskEntry.Id job.TaskEntry.Info.ReturnTypeName
                | Some name -> logger.Logf LogLevel.Info "Starting cloud task '%s' of type '%s'." name job.TaskEntry.Info.ReturnTypeName
                do! job.TaskEntry.DeclareStatus Running

            do! job.TaskEntry.IncrementJobCount()
            let sw = Stopwatch.StartNew()
            let! result = runJobAsync manager currentWorker joblt.FaultInfo job |> Async.Catch
            sw.Stop()

            match result with
            | Choice1Of2 () -> 
                logger.Logf LogLevel.Info "Completed job '%O' after %O" job.Id sw.Elapsed
                do! job.TaskEntry.IncrementCompletedJobCount()
                do! joblt.DeclareCompleted ()

            | Choice2Of2 e ->
                logger.Logf LogLevel.Error "Faulted job '%O' after %O\n%O" job.Id sw.Elapsed e
                do! joblt.DeclareFaulted (ExceptionDispatchInfo.Capture e)
                // declare job faulted to task manager
                do! joblt.TaskEntry.IncrementFaultedJobCount ()
    }
       

/// Defines a Cloud job evaluator that runs code within the current application domain
[<AutoSerializable(false)>]
type LocalJobEvaluator private (manager : IRuntimeManager, currentWorker : IWorkerId) =
    /// <summary>
    ///     Creates a new local job evaluator instance with provided runtime configuration.
    /// </summary>
    /// <param name="manager">Runtime manager object.</param>
    /// <param name="currentWorker">Current worker identifier.</param>
    static member Create(manager : IRuntimeManager, currentWorker : IWorkerId) =
        new LocalJobEvaluator(manager, currentWorker)

    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            return! JobEvaluator.loadAndRunJobAsync manager currentWorker assemblies jobtoken
        }

    interface IDisposable with
        member __.Dispose() = ()

/// Defines a Cloud job evaluator that runs in a managed pool of application domains.
/// Loading of assembly dependencies is performed by Vagabond, in a way where conflicting
/// dependencies will never be collocated in the same AppDomain.
[<AutoSerializable(false)>]
type AppDomainJobEvaluator private (configInitializer : DomainLocal<IRuntimeManager * IWorkerId>, pool : AppDomainEvaluatorPool) =

    /// <summary>
    ///     Creates a new AppDomain evaluator instance with provided parameters.
    /// </summary>
    /// <param name="initRuntimeConfig">AppDomain runtime configuration factory. Must be serializable lambda.</param>
    /// <param name="initializer">Optional domain initialization code. Is run before the configuration factory upon domain creation.</param>
    /// <param name="threshold">Timespan after which unused domain will be discarded.</param>
    /// <param name="minConcurrentDomains">Minimum permitted number of concurrent AppDomains.</param>
    /// <param name="maxConcurrentDomains">Maximum permitted number of concurrent AppDomains.</param>
    static member Create(initRuntimeConfig : unit -> IRuntimeManager * IWorkerId,
                                ?initializer : unit -> unit, ?threshold : TimeSpan, 
                                ?minConcurrentDomains : int, ?maxConcurrentDomains : int) =

        let domainInitializer () = initializer |> Option.iter (fun f -> f ())
        let pool = AppDomainEvaluatorPool.Create(domainInitializer, ?threshold = threshold, 
                                                    ?minimumConcurrentDomains = minConcurrentDomains,
                                                    ?maximumConcurrentDomains = maxConcurrentDomains)

        new AppDomainJobEvaluator(DomainLocal.Create initRuntimeConfig, pool)

    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            // avoid capturing evaluator in closure
            let configInitializer = configInitializer
            let eval () = async { 
                let manager, currentWorker = configInitializer.Value
                return! JobEvaluator.loadAndRunJobAsync manager currentWorker assemblies jobtoken 
            }

            return! pool.EvaluateAsync(jobtoken.TaskEntry.Info.Dependencies, eval ())
        }

    interface IDisposable with
        member __.Dispose () = (pool :> IDisposable).Dispose()