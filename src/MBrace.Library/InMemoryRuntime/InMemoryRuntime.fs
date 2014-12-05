namespace Nessos.MBrace.InMemoryRuntime

open Nessos.MBrace
open Nessos.MBrace.Continuation

type ConsoleLogger () =
    interface ICloudLogger with
        member __.Log(message : string) = System.Console.WriteLine message

type NullLogger () =
    interface ICloudLogger with
        member __.Log _ = ()

type InMemoryRuntime private (context : SchedulingContext, logger) =

    let taskId = System.Guid.NewGuid().ToString()

    static member Create (?logger : ICloudLogger) = 
        let logger = match logger with Some l -> l | None -> new NullLogger() :> _
        new InMemoryRuntime(ThreadParallel, logger)

    static member DefaultResources =
        resource { 
            yield InMemoryRuntime.Create () :> IRuntimeProvider
            yield InMemoryChannelProvider.CreateConfiguration()
            yield InMemoryAtomProvider.CreateConfiguration()
        }
        
    interface IRuntimeProvider with
        member __.ProcessId = "in memory process"
        member __.TaskId = taskId
        member __.Logger = logger
        member __.GetAvailableWorkers () = async {
            return raise <| new System.NotSupportedException("'GetAvailableWorkers not supported in InMemory runtime.")
        }

        member __.CurrentWorker =
            {
                new IWorkerRef with
                    member __.Type = "threadpool"
                    member __.Id = string <| System.Threading.Thread.CurrentThread.ManagedThreadId
            }

        member __.SchedulingContext = context
        member __.WithSchedulingContext newContext = 
            let newContext =
                match newContext with
                | Distributed -> ThreadParallel
                | c -> c

            new InMemoryRuntime(newContext, logger) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Sequential -> Sequential.Parallel computations
            | _ -> ThreadPool.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Sequential -> Sequential.Choice computations
            | _ -> ThreadPool.Choice computations

        member __.ScheduleStartChild (workflow, ?target:IWorkerRef, ?timeoutMilliseconds:int) = 
            match context with
            | Sequential -> Sequential.StartChild workflow
            | _ -> ThreadPool.StartChild(workflow, ?timeoutMilliseconds = timeoutMilliseconds)