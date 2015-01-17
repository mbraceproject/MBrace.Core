module internal MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.Thespian
open Nessos.Thespian.Remote

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime

open MBrace.SampleRuntime.Tasks

/// IWorkerRef implementation for the runtime
type Worker(procId : string) =
    let id = sprintf "sample runtime worker (pid %s)" procId
    interface IWorkerRef with
        member __.Id = id
        member __.Type = "sample runtime worker node"
        member __.CompareTo(other : obj) =
            match other with
            | :? Worker as w -> compare id (w :> IWorkerRef).Id
            | _ -> invalidArg "other" "invalid comparand."

    override __.ToString() = id
    override __.Equals other = 
        match other with
        | :? Worker as w -> id = (w :> IWorkerRef).Id
        | _ -> false

    override __.GetHashCode() = hash id

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

    let failTargetWorker () = invalidOp <| sprintf "Cannot target worker when running in '%A' execution context" context

    let extractComputations (computations : seq<Cloud<_> * IWorkerRef option>) =
        computations
        |> Seq.map (fun (c,w) -> if Option.isSome w then failTargetWorker () else c)
        |> Seq.toArray

    /// Creates a runtime provider instance for a provided task
    static member FromTask state procInfo dependencies (task : Task) =
        new RuntimeProvider(state, procInfo, dependencies, task.FaultPolicy, task.TaskId, Distributed)

    /// Creates a runtime provider instance for in-memory computation
    static member CreateInMemoryRuntime(state, procInfo : ProcessInfo) =
        new RuntimeProvider(state, procInfo, [], FaultPolicy.NoRetry, "ThreadPool", ThreadParallel)
        
    interface IRuntimeProvider with
        member __.ProcessId = procInfo.ProcessId
        member __.TaskId = taskId

        member __.SchedulingContext = context
        member __.WithSchedulingContext ctx =
            match ctx, context with
            | Distributed, (ThreadParallel | Sequential)
            | ThreadParallel, Sequential ->
                invalidOp <| sprintf "Cannot set scheduling context to '%A' when it already is '%A'." ctx context
            | _ ->
                new RuntimeProvider(state, procInfo, dependencies, faultPolicy, taskId, ctx) :> IRuntimeProvider

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, procInfo, dependencies, newPolicy, taskId, context) :> IRuntimeProvider

        member __.IsTargetedWorkerSupported = 
            match context with
            | Distributed -> true
            | _ -> false

        member __.ScheduleParallel computations = 
            match context with
            | Distributed -> Combinators.Parallel state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Parallel (extractComputations computations)
            | Sequential -> Sequential.Parallel (extractComputations computations)

        member __.ScheduleChoice computations = 
            match context with
            | Distributed -> Combinators.Choice state procInfo dependencies faultPolicy computations
            | ThreadParallel -> ThreadPool.Choice (extractComputations computations)
            | Sequential -> Sequential.Choice (extractComputations computations)

        member __.ScheduleStartChild(computation,worker,_) =
            match context with
            | Distributed -> Combinators.StartChild state procInfo dependencies faultPolicy worker computation
            | _ when Option.isSome worker -> failTargetWorker ()
            | ThreadParallel -> ThreadPool.StartChild computation
            | Sequential -> Sequential.StartChild computation

        member __.GetAvailableWorkers () = state.Workers.GetValue()
        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger