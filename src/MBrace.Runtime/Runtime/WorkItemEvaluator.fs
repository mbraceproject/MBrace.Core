namespace MBrace.Runtime

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open MBrace.Vagabond
open MBrace.Vagabond.LoadContextPool

open MBrace.Runtime.Utils

/// Work item evaluator abstraction
type ICloudWorkItemEvaluator =
    inherit IDisposable

    /// <summary>
    ///     Asynchronously evaluates a work item in the local worker.  
    /// </summary>
    /// <param name="dependencies">Local assemblies that the work item depends on.</param>
    /// <param name="workItemToken">Cloud work item token.</param>
    abstract Evaluate : dependencies:VagabondAssembly [] * workItemToken:ICloudWorkItemLeaseToken -> Async<unit>

[<RequireQualifiedAccess>]
module WorkItemEvaluator =

    /// <summary>
    ///     Asynchronously evaluates work item in the local application domain.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="currentWorker">Current worker executing  work item.</param>
    /// <param name="faultState">Work item fault state.</param>
    /// <param name="workItem">Work item instance to be executed.</param>
    let runWorkItemAsync (manager : IRuntimeManager) (currentWorker : IWorkerId) 
                    (faultState : CloudWorkItemFaultInfo) (workItem : CloudWorkItem) = async {

        let logger = manager.SystemLogger
        let jem = new WorkItemExecutionMonitor()
        use! distributionProvider = ParallelismProvider.Create(currentWorker, manager, workItem)
        let resources = resource {
            yield! manager.ResourceRegistry
            match workItem.Process.Info.AdditionalResources with Some r -> yield! r | None -> ()
            yield jem
            yield currentWorker
            yield manager
            yield distributionProvider :> IParallelismProvider
        }

        let ctx = { Resources = resources ; CancellationToken = workItem.CancellationToken }

        match faultState with
        | IsTargetedWorkItemOfDeadWorker (_,w) ->
            // always throw a fault exception if dead worker
            logger.Logf LogLevel.Info "Work item %O originally assigned to dead worker '%s'." workItem.Id w.Id
            let e = new FaultException(sprintf "Could not communicate with target worker '%O'." w)
            let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
            workItem.FaultCont ctx edi

        | FaultDeclaredByWorker(faultCount, latestError, w) ->
            logger.Logf LogLevel.Info "Work item %O faulted %d times while executed in worker '%O'." workItem.Id faultCount w
            // consult user-supplied fault policy to decide on further action
            let e = latestError.Reify(prepareForRaise = false)
            let msg = sprintf "Work item %O given up after it faulted %d times." workItem.Id faultCount
            let faultException = new FaultException(msg, e)

            match (try workItem.FaultPolicy.GetFaultRecoveryAction (faultCount, faultException) with e -> ThrowException e) with
            | ThrowException e ->
                let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                workItem.FaultCont ctx edi

            | Retry timeout ->
                do! Async.Sleep (int timeout.TotalMilliseconds)
                let faultData = { NumberOfFaults = faultCount ; FaultException = faultException }
                do workItem.StartWorkItem { ctx with Resources = resources.Register faultData }

        | WorkerDeathWhileProcessingWorkItem (faultCount, latestWorker) ->
            match workItem.TargetWorker with
            | Some tw when tw <> currentWorker ->
                // always throw a fault exception if targeted worker cannot be reached
                logger.Logf LogLevel.Info "Work item %O originally assigned to dead worker '%s'." workItem.Id tw.Id
                let e = new FaultException(sprintf "Could not communicate with target worker '%O'." tw)
                let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                workItem.FaultCont ctx edi

            | _ ->
                logger.Logf LogLevel.Info "Work item %O faulted %d times while being processed by nonresponsive worker '%O'." workItem.Id faultCount latestWorker
                // consult user-supplied fault policy to decide on further action
                let msg = sprintf "Work item '%O' was being processed by worker '%O' which has died." workItem.Id latestWorker
                let e = new FaultException(msg) :> exn
                match (try workItem.FaultPolicy.GetFaultRecoveryAction(faultCount, e) with e -> ThrowException e) with
                | ThrowException e -> 
                    let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                    workItem.FaultCont ctx edi
                | Retry timeout ->
                    do! Async.Sleep (int timeout.TotalMilliseconds)
                    let faultData = { NumberOfFaults = faultCount ; FaultException = e }
                    do workItem.StartWorkItem { ctx with Resources = resources.Register faultData }

        | NoFault ->  
            // no faults, proceed normally  
            do workItem.StartWorkItem ctx

        do! WorkItemExecutionMonitor.AwaitCompletion jem
    }
    
    /// <summary>
    ///     loads work item to local application domain and evaluates it locally.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="currentWorker">Current worker executing  work item.</param>
    /// <param name="assemblies">Vagabond assemblies to be used for computation.</param>
    /// <param name="workItemLease">Work item lease token.</param>
    let loadAndRunWorkItemAsync (manager : IRuntimeManager) (currentWorker : IWorkerId) 
                            (assemblies : VagabondAssembly []) (workItemLease : ICloudWorkItemLeaseToken) = async {

        ignore <| RuntimeManagerRegistry.TryRegister manager
        let logger = manager.SystemLogger
        logger.Logf LogLevel.Debug "Loading assembly dependencies for work item '%O'." workItemLease.Id
        for li in manager.AssemblyManager.LoadAssemblies assemblies do
            match li with
            | NotLoaded id -> logger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
            | LoadFault(id, e) -> logger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
            | Loaded _ -> ()

        logger.Logf LogLevel.Debug "Deserializing work item '%O'." workItemLease.Id
        let! workItemResult = workItemLease.GetWorkItem() |> Async.Catch
        match workItemResult with
        | Choice2Of2 e ->
            // failure to deserialize work item triggers special error handling;
            // trigger root cloud process as faulted without consulting fault policy.
            logger.Logf LogLevel.Error "Failed to deserialize work item '%O':\n%O" workItemLease.Id e
            let e = new FaultException(sprintf "Failed to deserialize work item '%O'." workItemLease.Id, e)
            let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
            do! workItemLease.Process.DeclareStatus CloudProcessStatus.Faulted
            let! _ = workItemLease.Process.TrySetResult(CloudProcessResult.Exception edi, currentWorker)
            do! workItemLease.DeclareCompleted()

        | Choice1Of2 workItem ->
            match workItemLease.FaultInfo with
            | WorkerDeathWhileProcessingWorkItem _ ->
                // Workers that died while processing work items almost certainly were not able to decrement
                // the active worker counter; use this as an interim fix, however it is not entirely correct.
                // need to think of a better design which should be incorporated in the cloud process/workItem hierarchies refactoring.
                do! workItem.Process.IncrementFaultedWorkItemCount()
            | _ -> ()

            if workItem.WorkItemType = CloudWorkItemType.ProcessRoot then
                match workItem.Process.Info.Name with
                | None -> logger.Logf LogLevel.Info "Starting cloud process '%s' of type '%s'." workItem.Process.Id workItem.Process.Info.ReturnTypeName
                | Some name -> logger.Logf LogLevel.Info "Starting cloud process '%s' of type '%s'." name workItem.Process.Info.ReturnTypeName
                do! workItem.Process.DeclareStatus CloudProcessStatus.Running

            do! workItem.Process.IncrementWorkItemCount()
            let sw = Stopwatch.StartNew()
            let! result = runWorkItemAsync manager currentWorker workItemLease.FaultInfo workItem |> Async.Catch
            sw.Stop()

            match result with
            | Choice1Of2 () -> 
                logger.Logf LogLevel.Info "Completed work item '%O' after %O" workItem.Id sw.Elapsed
                do! workItem.Process.IncrementCompletedWorkItemCount()
                do! workItemLease.DeclareCompleted ()

            | Choice2Of2 e ->
                logger.Logf LogLevel.Error "Faulted work item '%O' after %O\n%O" workItem.Id sw.Elapsed e
                let edi = ExceptionDispatchInfo.Capture(e, isFaultException = true)
                do! workItemLease.DeclareFaulted edi
                // declare work item faulted to cloud process manager
                do! workItemLease.Process.IncrementFaultedWorkItemCount ()
    }
       

/// Defines a Cloud work item evaluator that runs code within the current application domain
[<AutoSerializable(false)>]
type LocalWorkItemEvaluator private (manager : IRuntimeManager, currentWorker : IWorkerId) =
    /// <summary>
    ///     Creates a new local work item evaluator instance with provided runtime configuration.
    /// </summary>
    /// <param name="manager">Runtime manager object.</param>
    /// <param name="currentWorker">Current worker identifier.</param>
    static member Create(manager : IRuntimeManager, currentWorker : IWorkerId) =
        new LocalWorkItemEvaluator(manager, currentWorker)

    interface ICloudWorkItemEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], workItemToken:ICloudWorkItemLeaseToken) = async {
            return! WorkItemEvaluator.loadAndRunWorkItemAsync manager currentWorker assemblies workItemToken
        }

    interface IDisposable with
        member __.Dispose() = ()

/// Defines a Cloud work item evaluator that runs in a managed pool of application domains.
/// Loading of assembly dependencies is performed by Vagabond, in a way where conflicting
/// dependencies will never be collocated in the same AppDomain.
[<AutoSerializable(false)>]
type LoadContextWorkItemEvaluator private (configInitializer : DomainLocal<IRuntimeManager * IWorkerId>, pool : AssemblyLoadContextEvaluatorPool) =

    /// <summary>
    ///     Creates a new AppDomain evaluator instance with provided parameters.
    /// </summary>
    /// <param name="initRuntimeConfig">AppDomain runtime configuration factory. Must be serializable lambda.</param>
    /// <param name="initializer">Optional domain initialization code. Is run before the configuration factory upon domain creation.</param>
    /// <param name="threshold">Timespan after which unused domain will be discarded.</param>
    /// <param name="minConcurrentContexts">Minimum permitted number of concurrent LoadContexts.</param>
    /// <param name="maxConcurrentContexts">Maximum permitted number of concurrent LoadContexts.</param>
    static member Create(initRuntimeConfig : unit -> IRuntimeManager * IWorkerId,
                                ?initializer : unit -> unit, ?threshold : TimeSpan, 
                                ?minConcurrentContexts : int, ?maxConcurrentContexts : int) =

        let domainInitializer () = initializer |> Option.iter (fun f -> f ())
        let pool = AssemblyLoadContextEvaluatorPool.Create(domainInitializer, ?threshold = threshold, 
                                                    ?minimumConcurrentContexts = minConcurrentContexts,
                                                    ?maximumConcurrentContexts = maxConcurrentContexts)

        new LoadContextWorkItemEvaluator(DomainLocal.Create initRuntimeConfig, pool)

    interface ICloudWorkItemEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], workItemToken:ICloudWorkItemLeaseToken) = async {
            // avoid capturing evaluator in closure
            let configInitializer = configInitializer
            let eval () = async { 
                let manager, currentWorker = configInitializer.Value
                return! WorkItemEvaluator.loadAndRunWorkItemAsync manager currentWorker assemblies workItemToken 
            }

            return! pool.EvaluateAsync(workItemToken.Process.Info.Dependencies, eval ())
        }

    interface IDisposable with
        member __.Dispose () = (pool :> IDisposable).Dispose()