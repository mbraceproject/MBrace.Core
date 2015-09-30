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
            WorkItemEvaluator : ICloudWorkItemEvaluator
            StoreLogger : IRemoteSystemLogger
            StoreLoggerSubscription : IDisposable
        }
        member s.Dispose() =
            Disposable.dispose s.Agent
            Disposable.dispose s.WorkItemEvaluator
            Disposable.dispose s.LoggerSubscription
            Disposable.dispose s.StoreLoggerSubscription
            Disposable.dispose s.StoreLogger

    /// AppDomain configuration object that can be safely
    /// passed to AppDomain before it has been initialized
    /// State containing ActorRef's have been pickled in order to
    /// work around a Thespian issue where random listeners
    /// are being allocated in the event where ActorRef's are being
    /// deserialized before initialization.
    type private AppDomainConfig =
        {
            Logger : MarshaledLogger
            RuntimeState : Pickle<ClusterState>
            WorkingDirectory : string
            Hostname : string
        }
    
    /// <summary>
    ///     Initializes a worker agent and subscription to provided runtime state.
    /// </summary>
    /// <param name="useAppDomainIsolation">Enable AppDomain isolation for cloud work item execution.</param>
    /// <param name="logger">Logger bound to local worker process.s</param>
    /// <param name="maxConcurrentWorkItems">Maximum number of permitted concurrent work items.</param>
    /// <param name="state">MBrace.Thespian state object.</param>
    let initSubscription (useAppDomainIsolation : bool) (logger : ISystemLogger)
                            (heartBeatInterval : TimeSpan) (heartBeatThreshold : TimeSpan)
                            (maxConcurrentWorkItems : int) (state : ClusterState) = async {

        ignore Config.Serializer
        // it is important that the current worker id is initialized in the master AppDomain
        // and not in the worker domains. This ensures that all AppDomains identify with the master domain uri.
        let currentWorker = WorkerId.LocalInstance :> IWorkerId
        let manager = state.GetLocalRuntimeManager()
        let loggerSubscription = manager.LocalSystemLogManager.AttachLogger logger
        logger.LogInfo "Initializing worker store logger."
        let! storeLogger = manager.RuntimeSystemLogManager.CreateLogWriter(currentWorker)
        let storeLoggerSubscription = manager.LocalSystemLogManager.AttachLogger storeLogger

        let workItemEvaluator =
            if useAppDomainIsolation then
                logger.LogInfo "Initializing AppDomain pool evaluator."
                let domainConfig =
                    {
                        Logger = new MarshaledLogger(manager.SystemLogger)
                        RuntimeState = Config.Serializer.PickleTyped state
                        WorkingDirectory = Config.WorkingDirectory
                        Hostname = Config.HostName
                    }

                let initializer () =
                    Config.Initialize(populateDirs = false, isClient = false, hostname = domainConfig.Hostname, workingDirectory = domainConfig.WorkingDirectory)
                    Actor.Logger <- domainConfig.Logger
                    let domainName = System.AppDomain.CurrentDomain.FriendlyName
                    domainConfig.Logger.Logf LogLevel.Info "Initializing Application Domain '%s'." domainName
                    domainConfig.Logger.Logf LogLevel.Info "Thespian listening to %s on AppDomain '%s'." Config.LocalAddress domainName

                let managerF () =
                    // initializer has been run, safe to unpickle overall
                    let state = Config.Serializer.UnPickleTyped domainConfig.RuntimeState
                    let logger = domainConfig.Logger
                    let manager = state.GetLocalRuntimeManager()
                    let _ = manager.LocalSystemLogManager.AttachLogger(logger)
                    manager, currentWorker

                AppDomainWorkItemEvaluator.Create(managerF, initializer) :> ICloudWorkItemEvaluator
            else
                LocalWorkItemEvaluator.Create(manager, currentWorker) :> ICloudWorkItemEvaluator

        logger.LogInfo "Creating worker agent."
        let! agent = WorkerAgent.Create(manager, currentWorker, workItemEvaluator, maxConcurrentWorkItems, 
                                            submitPerformanceMetrics = true, heartbeatInterval = heartBeatInterval, heartbeatThreshold = heartBeatThreshold)
        do! agent.Start()
        return {
            Agent = agent
            LoggerSubscription = loggerSubscription
            WorkItemEvaluator = workItemEvaluator
            RuntimeState = state
            StoreLogger = storeLogger
            StoreLoggerSubscription = storeLoggerSubscription
        }
    }