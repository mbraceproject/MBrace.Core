namespace MBrace.SampleRuntime

open System

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

type private WorkerControl =
    | Subscribe of state:RuntimeState
    | UnSubscribe

type Worker private (actor : ActorRef<WorkerControl>) =

    member __.Subscribe(state : RuntimeState) = actor <-- Subscribe state
    member __.UnSubscribe () = actor <-- UnSubscribe

    static member InitLocal(logger : ISystemLogger, maxConcurrentJobs:int, useAppDomainIsolation:bool) =
        ignore Config.Serializer
        if maxConcurrentJobs < 1 then invalidArg "maxConcurrentJobs" "must be positive."
        let agent = WorkerAgent.Create()
        let behaviour (disposable : IDisposable option) (msg : WorkerControl) = async {
            disposable |> Option.iter (fun d -> d.Dispose())
            match msg with
            | Subscribe state when useAppDomainIsolation ->
                let workingDirectory = Config.WorkingDirectory 
                let initResourceManager () =
                    Config.Init(workingDirectory, cleanup = false)
                    new ResourceManager(state, logger) :> IRuntimeResourceManager

                let resourceManager = DomainLocal.Create initResourceManager

                let! wmanager = WorkerManager.Create(state.WorkerMonitor)
                let jobEvaluator = AppDomainJobEvaluator.Create(resourceManager)
                let config = 
                    {
                        MaxConcurrentJobs = maxConcurrentJobs
                        Resources = new ResourceManager(state, logger) :> IRuntimeResourceManager
                        WorkerManager = wmanager
                        JobEvaluator = jobEvaluator
                    }

                agent.Restart config
                return (Some (jobEvaluator :> IDisposable))

            | Subscribe state ->
                let resourceManager = new ResourceManager(state, logger)
                let! wmanager = WorkerManager.Create(state.WorkerMonitor)
                let jobEvaluator = new LocalJobEvaluator(resourceManager)
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
        new Worker(aref)