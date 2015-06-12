namespace MBrace.SampleRuntime

open System
open System.Threading

open Nessos.FsPickler
open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

/// Result value
type Result =
    | Completed of obj
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value : obj =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled e -> raise e

type private ResultCellMsg =
    | SetResult of Pickle<Result> * IReplyChannel<bool>
    | TryGetResult of IReplyChannel<Pickle<Result> option>

/// Defines a reference to a distributed result cell instance.
type TaskCompletionSource<'T> private (id : string, source : ActorRef<ResultCellMsg>, cts : ICloudCancellationTokenSource, taskInfo : CloudTaskInfo) =
    
    let lockObj = ref ()
    [<NonSerialized>]
    let mutable localAtom : CacheAtom<Result option> option = None
    [<NonSerialized>]
    let mutable localResult : Result option = None

    let tryGetResult () = async {
        match localResult with
        | Some _ as r -> return r
        | None ->
            let! result = source <!- TryGetResult
            return
                match result with
                | None -> None
                | Some rp ->
                    let r = Config.Serializer.UnPickleTyped rp |> Some
                    localResult <- r
                    r
    }

    let getCell () =
        match localAtom with
        | Some c -> c
        | None -> 
            lock lockObj (fun () ->
                match localAtom with
                | Some c -> c
                | None ->
                    let cell = CacheAtom.Create(tryGetResult (), intervalMilliseconds = 200)
                    localAtom <- Some cell
                    cell)

    /// Try setting the result
    member c.SetResult result = source <!- fun ch -> SetResult(Config.Serializer.PickleTyped result, ch)
    /// Try getting the result
    member c.TryGetResult () = getCell().GetValueAsync()
    /// Asynchronously poll for result
    member c.AwaitResult() = async {
        let! result = getCell().GetValueAsync()
        match result with
        | None -> 
            do! Async.Sleep 500
            return! c.AwaitResult()
        | Some r -> return r
    }   

    interface ICloudTask<'T> with
        member c.Id = id
        member c.AwaitResult(?timeout:int) = async {
            let! r = Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value :?> 'T
        }

        member c.TryGetResult() = async {
            let! r = c.TryGetResult()
            return r |> Option.map (fun r -> r.Value :?> 'T)
        }

        member c.IsCompleted = 
            match getCell().Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match getCell().Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match getCell().Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match getCell().Value with
            | Some (Completed _) -> Tasks.TaskStatus.RanToCompletion
            | Some (Exception _) -> Tasks.TaskStatus.Faulted
            | Some (Cancelled _) -> Tasks.TaskStatus.Canceled
            | None -> Tasks.TaskStatus.Running

        member c.Result =
            async {
                let! r = c.AwaitResult()
                return r.Value :?> 'T
            } |> Async.RunSync


    interface ICloudTaskCompletionSource<'T> with
        member x.Info = taskInfo

        member x.Type = typeof<'T>

        member x.CancellationTokenSource = cts

        member x.SetCompleted(t : 'T): Async<unit> = 
            x.SetResult(Completed t) |> Async.Ignore
        
        member x.SetException(edi: ExceptionDispatchInfo): Async<unit> = 
            x.SetResult(Exception edi) |> Async.Ignore
        
        member x.SetCancelled(exn: OperationCanceledException): Async<unit> =
            x.SetResult(Cancelled exn) |> Async.Ignore
        
        member x.Task: ICloudTask<'T> = x :> _

    /// Initialize a new result cell in the local process
    static member Init(cts, taskInfo) : TaskCompletionSource<'T> =
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
        new TaskCompletionSource<'T>(id, ref, cts, taskInfo)