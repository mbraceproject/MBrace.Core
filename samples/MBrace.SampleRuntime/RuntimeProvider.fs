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
open Nessos.MBrace.Store
open Nessos.MBrace.InMemoryRuntime
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


type ActorChannelProvider (state : RuntimeState) =
    let id = state.IPEndPoint.ToString()
    interface ICloudChannelProvider with
        member __.Name = "ActorChannel"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateChannel<'T> (_ : string) = async {
            let! ch = state.ResourceFactory.RequestChannel<'T> ()
            return ch :> ISendPort<'T>, ch :> IReceivePort<'T>
        }

        member __.DisposeContainer _ = async.Zero()
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, procInfo : ProcessInfo, dependencies, faultPolicy, taskId, context) =

    /// Creates a runtime provider instance for a provided task
    static member FromTask state procInfo dependencies (task : Task) =
        new RuntimeProvider(state, procInfo, dependencies, task.FaultPolicy, task.TaskId, Distributed)
        
    interface IRuntimeProvider with
        member __.ProcessId = procInfo.ProcessId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext context = 
            new RuntimeProvider(state, procInfo, dependencies, faultPolicy, taskId, context) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, procInfo, dependencies, newPolicy, taskId, context) :> IRuntimeProvider

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel computations
            | Sequential -> Sequential.Parallel computations

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice computations
            | Sequential -> Sequential.Choice computations

        member __.ScheduleStartChild(computation,_,_) =
            match context with
            | Distributed -> Combinators.StartChild state procInfo dependencies faultPolicy computation
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = state.Workers.GetValue()
        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger
