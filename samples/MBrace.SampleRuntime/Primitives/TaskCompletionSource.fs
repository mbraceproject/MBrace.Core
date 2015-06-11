namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

/// Result value
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled e -> raise e

type private ResultCellMsg<'T> =
    | SetResult of Result<'T> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Result<'T> option>

/// Defines a reference to a distributed result cell instance.
type TaskCompletionSource<'T> private (id : string, source : ActorRef<ResultCellMsg<'T>>) as self =
    [<NonSerialized>]
    let mutable localCell : CacheAtom<Result<'T> option> option = None
    let getLocalCell() =
        match localCell with
        | Some c -> c
        | None ->
            lock self (fun () ->
                let cell = CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)
                localCell <- Some cell
                cell)

    /// Try setting the result
    member c.SetResult result = source <!- fun ch -> SetResult(result, ch)
    /// Try getting the result
    member c.TryGetResult () = source <!- TryGetResult
    /// Asynchronously poll for result
    member c.AwaitResult() = async {
        let! result = source <!- TryGetResult
        match result with
        | None -> 
            do! Async.Sleep 500
            return! c.AwaitResult()
        | Some r -> return r
    }   

    interface ICloudTask<'T> with
        member c.Id = id
        member c.AwaitResult(?timeout:int) = local {
            let! r = Cloud.OfAsync <| Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value
        }

        member c.TryGetResult() = local {
            let! r = Cloud.OfAsync <| c.TryGetResult()
            return r |> Option.map (fun r -> r.Value)
        }

        member c.IsCompleted = 
            match getLocalCell().Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match getLocalCell().Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match getLocalCell().Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match getLocalCell().Value with
            | Some (Completed _) -> Tasks.TaskStatus.RanToCompletion
            | Some (Exception _) -> Tasks.TaskStatus.Faulted
            | Some (Cancelled _) -> Tasks.TaskStatus.Canceled
            | None -> Tasks.TaskStatus.Running

        member c.Result = 
            async {
                let! r = c.AwaitResult()
                return r.Value
            } |> Async.RunSync


    interface ICloudTaskCanceller with
        member x.SetCancelled(exn: OperationCanceledException): Async<unit> = 
            x.SetResult(Cancelled exn) |> Async.Ignore
        
        member x.SetException(edi: ExceptionDispatchInfo): Async<unit> = 
            x.SetResult(Exception edi) |> Async.Ignore
        
    interface IAsyncDisposable with
        member x.Dispose(): Async<unit> = async.Zero()

    interface ICloudTaskCompletionSource<'T> with

        member x.SetCompleted(t : 'T): Async<unit> = 
            x.SetResult(Completed t) |> Async.Ignore
        
        member x.SetException(edi: ExceptionDispatchInfo): Async<unit> = 
            x.SetResult(Exception edi) |> Async.Ignore
        
        member x.SetCancelled(exn: OperationCanceledException): Async<unit> =
            x.SetResult(Cancelled exn) |> Async.Ignore
        
        member x.Task: ICloudTask<'T> = x :> _

        member x.Canceller = x :> _
        

    /// Initialize a new result cell in the local process
    static member Init() : TaskCompletionSource<'T> =
        let behavior state msg = async {
            match msg with
            | SetResult (_, rc) when Option.isSome state -> 
                do! rc.Reply false
                return state
            | SetResult (result, rc) ->
                do! rc.Reply true
                return (Some result)

            | TryGetResult rc ->
                do! rc.Reply state
                return state
        }

        let ref =
            Actor.Stateful None behavior
            |> Actor.Publish
            |> Actor.ref

        let id = Guid.NewGuid().ToString()
        new TaskCompletionSource<'T>(id, ref)