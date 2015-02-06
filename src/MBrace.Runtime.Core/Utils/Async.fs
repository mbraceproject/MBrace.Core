namespace MBrace.Runtime.Utils

open System
open System.Threading
open System.Threading.Tasks

open MBrace.Continuation

[<AutoOpen>]
module AsyncUtils =

    type Async with
        
        /// <summary>
        ///     Runs provided workflow with timeout.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        static member WithTimeout(workflow : Async<'T>, timeoutMilliseconds:int) : Async<'T> = async {
            if timeoutMilliseconds = 0 then return raise <| new TimeoutException()
            elif timeoutMilliseconds = Timeout.Infinite then return! workflow
            else
                let! ct = Async.CancellationToken
                let tcs = TaskCompletionSource<'T> ()
                let timeoutCallback _ = tcs.SetException(new TimeoutException())
                let _ = new Timer(timeoutCallback, null, timeoutMilliseconds, Timeout.Infinite)
                do Async.StartWithContinuations(workflow, tcs.TrySetResult >> ignore, tcs.TrySetException >> ignore, ignore >> tcs.TrySetCanceled >> ignore, ct)
                return! tcs.Task.AwaitResultAsync()
        }

        /// <summary>
        ///     Asynchronously await an IObservable to yield results.
        ///     Returns the first value to be observed.
        /// </summary>
        /// <param name="observable">Observable source.</param>
        /// <param name="timeout">Timeout in milliseconds. Defaults to no timeout.</param>
        static member AwaitObservable(observable: IObservable<'T>, ?timeoutMilliseconds:int) =
            let tcs = new TaskCompletionSource<'T>()
            let rec observer t = tcs.TrySetResult t |> ignore ; remover.Dispose()
            and remover : IDisposable = observable.Subscribe observer

            match timeoutMilliseconds with
            | None -> tcs.Task.AwaitResultAsync()
            | Some t -> Async.WithTimeout(tcs.Task.AwaitResultAsync(), t)