namespace MBrace.Runtime.Utils

open System
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module TaskUtils =

    type Task<'T> with

        /// <summary>
        ///     Create a new task that times out after a given amount of milliseconds.
        /// </summary>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        member t.WithTimeout(timeoutMilliseconds:int) : Task<'T> =
            if timeoutMilliseconds = Timeout.Infinite then t
            else
                let tcs = new TaskCompletionSource<'T>()
                let onCompletion (t : Task<'T>) =
                    match t.Status with
                    | TaskStatus.RanToCompletion -> tcs.TrySetResult t.Result |> ignore
                    | TaskStatus.Faulted -> tcs.TrySetException t.Exception.InnerExceptions |> ignore
                    | TaskStatus.Canceled -> tcs.TrySetCanceled () |> ignore
                    | _ -> ()

                let timerCallBack _ = tcs.TrySetException(new TimeoutException() :> exn) |> ignore

                let _ = t.ContinueWith onCompletion
                let timer = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                tcs.Task

        /// <summary>
        ///     Create a new task that times out after a given amount of milliseconds.
        /// </summary>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        member t.TryWithTimeout(timeoutMilliseconds:int) : Task<'T option> =
            let tcs = new TaskCompletionSource<'T option>()
            let onCompletion (t : Task<'T>) =
                if t.IsCompleted then tcs.TrySetResult (Some t.Result) |> ignore
                elif t.IsCanceled then tcs.TrySetCanceled () |> ignore
                elif t.IsFaulted then tcs.TrySetException t.Exception.InnerExceptions |> ignore

            let _ = t.ContinueWith onCompletion

            if timeoutMilliseconds <> Timeout.Infinite then
                let timerCallBack _ = tcs.TrySetResult None |> ignore
                let timer = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                ()

            tcs.Task