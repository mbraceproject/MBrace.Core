namespace MBrace.SampleRuntime

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private WorkerControl =
    | Subscribe of state:RuntimeState
    | UnSubscribe

type Worker private (actor : ActorRef<WorkerControl>, logger : ICloudLogger) =

    member __.Subscribe(state : RuntimeState) = actor <-- Subscribe state
    member __.UnSubscribe () = actor <-- UnSubscribe

    static member InitLocal(logger : ICloudLogger, maxConcurrentJobs:int, useAppDomainIsolation:bool) =
        ignore Config.Serializer
        if maxConcurrentJobs < 1 then invalidArg "maxConcurrentJobs" "must be positive."
        let agent = WorkerAgent.Create()
        let behaviour (disposable : IDisposable option) (msg : WorkerControl) = async {
            disposable |> Option.iter (fun d -> d.Dispose())
            match msg with
            | Subscribe state when useAppDomainIsolation ->
                let appDomainInitializer = let wd = Config.WorkingDirectory in fun () -> Config.Init(wd, cleanup = false)
                let resourceManager = 
                    DomainLocal.Create(fun () -> 
                                                { SystemLogger = logger ; 
                                                  ResourceManager = new ResourceManager(state, logger) :> IRuntimeResourceManager })

                let! wmanager = WorkerManager.Create(state.WorkerMonitor)
                let jobEvaluator = AppDomainJobEvaluator.Create(resourceManager, initializer = appDomainInitializer)
                let config = 
                    {
                        MaxConcurrentJobs = maxConcurrentJobs ; 
                        Resources = resourceManager.Value.ResourceManager ; 
                        WorkerManager = wmanager ;
                        JobEvaluator = jobEvaluator ;
                    }

                agent.Restart config
                return (Some (jobEvaluator :> IDisposable))

            | Subscribe state ->
                let resourceManager = new ResourceManager(state, logger)
                let! wmanager = WorkerManager.Create(state.WorkerMonitor)
                let jobEvaluator = new LocalJobEvaluator(resourceManager, logger)
                let config =
                    {
                        MaxConcurrentJobs = maxConcurrentJobs ;
                        Resources = resourceManager ;
                        WorkerManager = wmanager
                        JobEvaluator = jobEvaluator
                    }

                agent.Restart config
                return None

            | UnSubscribe -> 
                try agent.Stop() with _ -> ()
                return None
        }

        let aref = Actor.Stateful None behaviour |> Actor.Publish |> Actor.ref
        new Worker(aref, logger)