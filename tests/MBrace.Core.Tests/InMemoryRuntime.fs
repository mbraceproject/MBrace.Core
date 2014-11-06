namespace Nessos.MBrace.Tests

open Nessos.MBrace
open Nessos.MBrace.Library
open Nessos.MBrace.Runtime

type InMemoryRuntime private (context : SchedulingContext) =

    let taskId = System.Guid.NewGuid().ToString()

    static member Create () = new InMemoryRuntime(ThreadParallel)

    static member Resource =
        resource { yield InMemoryRuntime.Create () :> IRuntimeProvider }
        
    interface IRuntimeProvider with
        member __.ProcessId = "in memory process"
        member __.TaskId = taskId
        member __.Logger = { new ICloudLogger with member __.Log _ = () }
        member __.GetAvailableWorkers () = async {
            return raise <| new System.NotSupportedException("'GetAvailableWorkers not supported in InMemory runtime.")
        }

        member __.CurrentWorker =
            raise <| new System.NotSupportedException("'GetAvailableWorkers not supported in InMemory runtime.")

        member __.SchedulingContext = context
        member __.WithSchedulingContext newContext = 
            let newContext =
                match newContext with
                | Distributed -> ThreadParallel
                | c -> c

            new InMemoryRuntime(newContext) :> IRuntimeProvider

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