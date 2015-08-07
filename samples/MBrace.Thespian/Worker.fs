namespace MBrace.Thespian.Runtime

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library
open MBrace.Runtime

module Worker =
    
    /// <summary>
    ///     Initializes a worker agent that subscribes to given runtime state
    /// </summary>
    /// <param name="useAppDomainIsolation">Enable AppDomain isolation for cloud job execution.</param>
    /// <param name="state">MBrace.Thespian state object.</param>
    /// <param name="logger">Logger bound to local worker process.s</param>
    /// <param name="maxConcurrentJobs">Maximum number of permitted concurrent jobs.</param>
    let initialize (useAppDomainIsolation : bool) (state : RuntimeState)
                        (logger : ISystemLogger) (maxConcurrentJobs : int) = async {

        ignore Config.Serializer
        let currentWorker = WorkerId.LocalInstance :> IWorkerId
        let manager = state.GetLocalRuntimeManager()
        let _ = manager.AttachSystemLogger logger

        let jobEvaluator =
            if useAppDomainIsolation then
                logger.LogInfo "Initializing AppDomain pool evaluator."
                let workingDirectory = Config.WorkingDirectory 
                let initializer () =
                    Config.Initialize(populateDirs = false) 
                    logger.Logf LogLevel.Info "Initializing Application Domain '%s'." System.AppDomain.CurrentDomain.FriendlyName

                let managerF () =
                    let manager = state.GetLocalRuntimeManager()
                    let _ = manager.AttachSystemLogger(logger)
                    manager, currentWorker

                AppDomainJobEvaluator.Create(managerF, initializer) :> ICloudJobEvaluator
            else
                LocalJobEvaluator.Create(manager, currentWorker) :> ICloudJobEvaluator

        logger.LogInfo "Creating worker agent."
        let! agent = WorkerAgent.Create(manager, currentWorker, jobEvaluator, maxConcurrentJobs, submitPerformanceMetrics = true)
        do! agent.Start()
        return agent
    }