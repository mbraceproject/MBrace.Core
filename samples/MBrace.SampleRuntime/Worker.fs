module internal MBrace.SampleRuntime.Worker

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

let initialize (useAppDomainIsolation : bool) (state : RuntimeState)
                    (logger : ISystemLogger) (maxConcurrentJobs : int) = async {

    ignore Config.Serializer
    let resourceManager = new ResourceManager(state, logger)

    let jobEvaluator =
        if useAppDomainIsolation then
            logger.LogInfo "Initializing AppDomain pool evaluator."
            let workingDirectory = Config.WorkingDirectory 
            let initializer () =
                Config.Init(populateDirs = false) 
                logger.Logf LogLevel.Info "Initializing Application Domain '%s'." System.AppDomain.CurrentDomain.FriendlyName

            let managerF = DomainLocal.Create(fun () -> new ResourceManager(state, logger) :> IRuntimeResourceManager)

            AppDomainJobEvaluator.Create(managerF, initializer) :> ICloudJobEvaluator
        else
            new LocalJobEvaluator(resourceManager) :> ICloudJobEvaluator

    logger.LogInfo "Creating worker agent."
    let agent = WorkerAgent.Create(resourceManager, jobEvaluator, maxConcurrentJobs)
    logger.LogInfo "Setting up worker subscription."
    let! subscription = WorkerSubscriptionManager.Init(state.WorkerMonitor, agent)
    do! agent.Start()
    return agent, subscription
}