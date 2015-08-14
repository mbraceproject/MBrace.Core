namespace MBrace.Thespian.Runtime

open System

open Nessos.Thespian
open Nessos.FsPickler

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
            RuntimeState : ClusterState
            Agent : WorkerAgent
            LoggerSubscription : IDisposable
            JobEvaluator : ICloudJobEvaluator
        }
    with
        member s.Dispose() =
            Disposable.dispose s.Agent
            Disposable.dispose s.JobEvaluator
            Disposable.dispose s.LoggerSubscription

    /// AppDomain configuration object that can be safely
    /// passed to AppDomain before it has been initialized
    /// State containing ActorRef's have been pickled in order to
    /// work around a Thespian issue where random listeners
    /// are being allocated in the event where ActorRef's are being
    /// deserialized before initialization.
    type private AppDomainConfig =
        {
            Logger : Pickle<ActorLogger>
            RuntimeState : Pickle<ClusterState>
            WorkingDirectory : string
            Hostname : string
        }
    
    /// <summary>
    ///     Initializes a worker agent and subscription to provided runtime state.
    /// </summary>
    /// <param name="useAppDomainIsolation">Enable AppDomain isolation for cloud job execution.</param>
    /// <param name="logger">Logger bound to local worker process.s</param>
    /// <param name="maxConcurrentJobs">Maximum number of permitted concurrent jobs.</param>
    /// <param name="state">MBrace.Thespian state object.</param>
    let initSubscription (useAppDomainIsolation : bool) (logger : ISystemLogger) 
                            (maxConcurrentJobs : int) (state : ClusterState) = async {

        ignore Config.Serializer
        // it is important that the current worker id is initialized in the master AppDomain
        // and not in the worker domains. This ensures that all workers identify with the master uri.
        let currentWorker = WorkerId.LocalInstance :> IWorkerId
        let manager = state.GetLocalRuntimeManager()
        let loggerSubscription = manager.AttachSystemLogger logger

        let jobEvaluator =
            if useAppDomainIsolation then
                logger.LogInfo "Initializing AppDomain pool evaluator."
                let domainConfig =
                    {
                        Logger = logger |> ActorLogger.Create |> Config.Serializer.PickleTyped
                        RuntimeState = Config.Serializer.PickleTyped state
                        WorkingDirectory = Config.WorkingDirectory
                        Hostname = Config.HostName
                    }

                let initializer () =
                    Config.Initialize(populateDirs = false, hostname = domainConfig.Hostname, workingDirectory = domainConfig.WorkingDirectory)
                    // Thespian initialized, safe to unpickle actor refs
                    let logger = Config.Serializer.UnPickleTyped domainConfig.Logger
                    Actor.Logger <- logger
                    let domainName = System.AppDomain.CurrentDomain.FriendlyName
                    logger.Logf LogLevel.Info "Initializing Application Domain '%s'." domainName
                    logger.Logf LogLevel.Info "Thespian listening to %s on AppDomain '%s'." Config.LocalAddress domainName

                let managerF () =
                    // initializer has been run, safe to unpickle overall
                    let state = Config.Serializer.UnPickleTyped domainConfig.RuntimeState
                    let logger = Config.Serializer.UnPickleTyped domainConfig.Logger
                    let manager = state.GetLocalRuntimeManager()
                    let _ = manager.AttachSystemLogger(logger)
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