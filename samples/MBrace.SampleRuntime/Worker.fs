module internal MBrace.SampleRuntime.Worker

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

let initialize (useAppDomainIsolation : bool) (state : RuntimeState)
                    (logger : ISystemLogger) (maxConcurrentJobs : int) = async {

    ignore Config.Serializer
    let resourceManager = new RuntimeManager(state, logger)
    let currentWorker = WorkerId.LocalInstance :> IWorkerId

    let jobEvaluator =
        if useAppDomainIsolation then
            logger.LogInfo "Initializing AppDomain pool evaluator."
            let workingDirectory = Config.WorkingDirectory 
            let initializer () =
                Config.Init(populateDirs = false) 
                logger.Logf LogLevel.Info "Initializing Application Domain '%s'." System.AppDomain.CurrentDomain.FriendlyName

            let managerF = DomainLocal.Create(fun () -> new RuntimeManager(state, logger) :> IRuntimeManager, currentWorker)

            AppDomainJobEvaluator.Create(managerF, initializer) :> ICloudJobEvaluator
        else
            new LocalJobEvaluator(resourceManager, currentWorker) :> ICloudJobEvaluator

    logger.LogInfo "Creating worker agent."
    let! agent = WorkerAgent.Create(resourceManager, currentWorker, jobEvaluator, maxConcurrentJobs, submitPerformanceMetrics = true)
    do! agent.Start()
    return agent
}