namespace MBrace.Thespian.Runtime

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Runtime
open MBrace.Runtime.Utils

module internal WorkerSubscription =

    /// Disposable MBrace worker subscription state object
    [<NoEquality; NoComparison; AutoSerializable(false)>]
    type Subscription =
        {
            RuntimeState : RuntimeState
            Agent : WorkerAgent
            LoggerSubscription : IDisposable
            JobEvaluator : ICloudJobEvaluator
        }
    with
        member s.Dispose() =
            Disposable.dispose s.Agent
            Disposable.dispose s.JobEvaluator
            Disposable.dispose s.LoggerSubscription
    
    /// <summary>
    ///     Initializes a worker agent and subscription to provided runtime state.
    /// </summary>
    /// <param name="useAppDomainIsolation">Enable AppDomain isolation for cloud job execution.</param>
    /// <param name="logger">Logger bound to local worker process.s</param>
    /// <param name="maxConcurrentJobs">Maximum number of permitted concurrent jobs.</param>
    /// <param name="state">MBrace.Thespian state object.</param>
    let initSubscription (useAppDomainIsolation : bool) (logger : ISystemLogger) 
                            (maxConcurrentJobs : int) (state : RuntimeState) = async {

        ignore Config.Serializer
        let currentWorker = WorkerId.LocalInstance :> IWorkerId
        let manager = state.GetLocalRuntimeManager()
        let loggerSubscription = manager.AttachSystemLogger logger

        let jobEvaluator =
            if useAppDomainIsolation then
                logger.LogInfo "Initializing AppDomain pool evaluator."
                // use serializable logger for AppDomains
                let actorLogger = ActorLogger.Create(logger)
                let workingDirectory = Config.WorkingDirectory 
                let initializer () =
                    Config.Initialize(populateDirs = false)
                    Actor.Logger <- actorLogger
                    actorLogger.Logf LogLevel.Info "Initializing Application Domain '%s'." System.AppDomain.CurrentDomain.FriendlyName

                let managerF () =
                    let manager = state.GetLocalRuntimeManager()
                    let _ = manager.AttachSystemLogger(actorLogger)
                    manager, currentWorker

                AppDomainJobEvaluator.Create(managerF, initializer) :> ICloudJobEvaluator
            else
                LocalJobEvaluator.Create(manager, currentWorker) :> ICloudJobEvaluator

        logger.LogInfo "Creating worker agent."
        let! agent = WorkerAgent.Create(manager, currentWorker, jobEvaluator, maxConcurrentJobs, submitPerformanceMetrics = true)
        do! agent.Start()
        return {
            Agent = agent
            LoggerSubscription = loggerSubscription
            JobEvaluator = jobEvaluator
            RuntimeState = state
        }
    }