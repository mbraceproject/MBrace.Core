namespace MBrace.Runtime

open MBrace.Runtime.Utils

[<AbstractClass; AutoSerializable(false)>]
type Process internal (tcs : ICloudTaskCompletionSource, manager : ICloudTaskManager) =
    let cell = CacheAtom.Create(async { return! manager.GetTaskState(tcs.Info.Id) }, intervalMilliseconds = 200)

    abstract AwaitResultBoxed : ?timeoutMilliseconds:int -> Async<obj>
    abstract TryGetResultBoxed : unit -> Async<obj option>
    abstract ResultBoxed : obj

    member __.Status = cell.Value.Status
    member __.Id = tcs.Info.Id
    member __.Name = tcs.Info.Name
    member __.Type = tcs.Type
    member __.Cancel() = tcs.CancellationTokenSource.Cancel()

//[<Sealed; AutoSerializable(false)>]
//type Process<'T> internal (tcs : ICloudTaskCompletionSource<'T>, manager : ICloudTaskManager) =
//    inherit Process(tcs, manager)
//
//    member __.Task = tcs.Task
//    member __.AwaitResult(?timeoutMilliseconds:int) = tcs.Task.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
//    member __.TryGetResult() = tcs.Task.TryGetResult()

//    override __.AwaitResultBoxed(?timeoutMilliseconds:int) = tcs.Task