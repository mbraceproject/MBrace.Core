module internal Nessos.MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.Tasks

type RuntimeProvider private (state : RuntimeState, taskId : string, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state dependencies (task : Task) =
        new RuntimeProvider(state, task.Id, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = "0"
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state dependencies computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state dependencies computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state dependencies computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async {
            return raise <| System.NotImplementedException()
        }

        member __.CurrentWorker = raise <| System.NotImplementedException()
        member __.Logger = raise <| System.NotImplementedException()