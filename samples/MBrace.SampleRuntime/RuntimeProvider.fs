module internal Nessos.MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.MBrace
open Nessos.MBrace.InMemory
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.Tasks

type Worker(proc : Process) =
    let id = sprintf "sample runtime worker (pid %d)" proc.Id
    interface IWorkerRef with
        member __.Id = id
        member __.Type = "sample runtime worker node"

    static member LocalWorker = new Worker(Process.GetCurrentProcess())
        

type RuntimeProvider private (state : RuntimeState, procId : string, taskId : string, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state procId dependencies (task : Task) =
        new RuntimeProvider(state, procId, task.TaskId, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = procId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, procId, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procId dependencies computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procId dependencies computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state procId dependencies computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = async {
            return! state.Workers.GetValue()
        }

        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger