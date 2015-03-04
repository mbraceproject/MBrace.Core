﻿module internal MBrace.SampleRuntime.RuntimeProvider

//
//  Implements the scheduling context for sample runtime.
//

#nowarn "444"

open System.Diagnostics

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Thespian.Remote

open Nessos.Vagabond

open MBrace
open MBrace.Continuation
open MBrace.Workflows
open MBrace.Runtime.InMemory
open MBrace.Runtime.Vagabond
open MBrace.Store
open MBrace.Runtime

open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Types

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


type ActorAtomProvider (state : RuntimeState) =
    let id = state.IPEndPoint.ToString()
    interface ICloudAtomProvider with
        member x.CreateAtom(container: string, initValue: 'T): Async<ICloudAtom<'T>> = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! atom = state.ResourceFactory.RequestAtom<'T>(id, initValue)
            return atom :> ICloudAtom<'T>
        }

        member x.CreateUniqueContainerName () = System.Guid.NewGuid().ToString()

        member x.DisposeContainer (_ : string) = async.Zero()
        
        member x.Id: string = id
        
        member x.IsSupportedValue(value: 'T): bool = true
        
        member x.Name: string = "ActorAtom"

type ActorChannelProvider (state : RuntimeState) =
    let id = state.IPEndPoint.ToString()
    interface ICloudChannelProvider with
        member __.Name = "ActorChannel"
        member __.Id = id
        member __.CreateUniqueContainerName () = ""

        member __.CreateChannel<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| System.Guid.NewGuid().ToString()
            let! ch = state.ResourceFactory.RequestChannel<'T> id
            return ch :> ISendPort<'T>, ch :> IReceivePort<'T>
        }

        member __.DisposeContainer _ = async.Zero()
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, procInfo : ProcessInfo, dependencies : AssemblyId [], faultPolicy, jobId) =

    static let mkNestedCts elevate (ct : ICloudCancellationToken) =
        let parentCts = ct :?> DistributedCancellationTokenSource
        let dcts = DistributedCancellationTokenSource.CreateLinkedCancellationTokenSource(parentCts, forceElevation = elevate)
        dcts :> ICloudCancellationTokenSource

    /// Creates a runtime provider instance for a provided job
    static member FromJob state dependencies (job : Job) =
        new RuntimeProvider(state, job.ProcessInfo, dependencies, job.FaultPolicy, job.JobId)
        
    interface ICloudRuntimeProvider with
        member __.ProcessId = procInfo.ProcessId
        member __.JobId = jobId

        member __.FaultPolicy = faultPolicy
        member __.WithFaultPolicy newPolicy = 
            new RuntimeProvider(state, procInfo, dependencies, newPolicy, jobId) :> ICloudRuntimeProvider

        member __.CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken[]) = async {
            match parents with
            | [||] -> let! cts = state.ResourceFactory.RequestCancellationTokenSource() in return cts :> _
            | [| ct |] -> return mkNestedCts false ct
            | _ -> return raise <| new System.NotSupportedException("Linking multiple cancellation tokens not supported in this runtime.")
        }

        member __.IsTargetedWorkerSupported = true

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts false, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts false, computations)

        member __.ScheduleParallel computations =
            Combinators.Parallel state procInfo dependencies faultPolicy computations

        member __.ScheduleChoice computations = 
            Combinators.Choice state procInfo dependencies faultPolicy computations

        member __.ScheduleStartAsTask(workflow : Workflow<'T>, faultPolicy, cancellationToken, ?target:IWorkerRef) =
            Combinators.StartAsCloudTask state procInfo dependencies cancellationToken faultPolicy target workflow

        member __.GetAvailableWorkers () = state.Workers.GetValue()
        member __.CurrentWorker = Worker.LocalWorker :> IWorkerRef
        member __.Logger = state.Logger :> ICloudLogger