module internal Nessos.MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.Thespian
open Nessos.Thespian.Remote

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Library
open Nessos.MBrace.Runtime

open Nessos.MBrace.SampleRuntime.Tasks

/// IWorkerRef implementation for the runtime
type Worker(procId : string) =
    let id = sprintf "sample runtime worker (id %s)" procId
    interface IWorkerRef with
        member __.Id = id
        member __.Type = "sample runtime worker node"

    static member LocalWorker = new Worker(Process.GetCurrentProcess().Id.ToString())
    static member RemoteWorker(id: string) = new Worker(id)


type ChannelProvider (state : RuntimeState) =
    interface ICloudChannelProvider with
        member __.CreateChannel<'T> () = async {
            let! ch = state.ResourceFactory.RequestChannel<'T> ()
            return ch :> ISendPort<'T>, ch :> IReceivePort<'T>
        }
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, procId, container, taskId, dependencies, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state procId dependencies (task : Task) =
        new RuntimeProvider(state, procId, task.Container, task.TaskId, dependencies, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = procId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, procId, container, taskId, dependencies, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procId container dependencies computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procId container dependencies computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state procId container dependencies computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = state.Workers.GetValue()
        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger
