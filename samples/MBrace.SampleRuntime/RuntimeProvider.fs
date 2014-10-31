module internal Nessos.MBrace.SampleRuntime.RuntimeProvider

#nowarn "444"

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.RuntimeTypes

type RuntimeProvider private (state : RuntimeState, taskId : string, dependencies, context) =

    static member FromTask state dependencies (task : Task) =
        new RuntimeProvider(state, task.Id, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = taskId
        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()

        member __.Logger = raise <| System.NotImplementedException()

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = new RuntimeProvider(state, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.scheduleParallel state dependencies computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.scheduleChoice state dependencies computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.scheduleStartChild state dependencies computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation