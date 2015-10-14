namespace MBrace.Runtime.Utils

open System
open System.Threading
open System.Threading.Tasks

open MBrace.Core.Internals

[<AutoOpen>]
module AsyncUtils =

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            __.Bind(Async.AwaitTaskCorrect f, g)
        member inline __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> =
            __.Bind(Async.AwaitTaskCorrect (f.ContinueWith ignore), g)
        member inline __.ReturnFrom(f : Task<'T>) : Async<'T> =
            __.ReturnFrom(Async.AwaitTaskCorrect f)
        member inline __.ReturnFrom(f : Task) : Async<unit> =
            __.ReturnFrom(Async.AwaitTaskCorrect (f.ContinueWith ignore))

    type Async with
        
        /// <summary>
        ///     Runs provided workflow with timeout.
        /// </summary>
        /// <param name="workflow">Workflow to be executed.</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        static member WithTimeout(workflow : Async<'T>, ?timeoutMilliseconds:int) : Async<'T> = async {
            let timeoutMilliseconds = defaultArg timeoutMilliseconds Timeout.Infinite
            if timeoutMilliseconds = 0 then return raise <| new TimeoutException()
            elif timeoutMilliseconds = Timeout.Infinite then return! workflow
            else
                let! ct = Async.CancellationToken
                let tcs = TaskCompletionSource<'T> ()
                let timeoutCallback _ = tcs.SetException(new TimeoutException())
                let _ = new Timer(timeoutCallback, null, timeoutMilliseconds, Timeout.Infinite)
                do Async.StartWithContinuations(workflow, tcs.TrySetResult >> ignore, tcs.TrySetException >> ignore, ignore >> tcs.TrySetCanceled >> ignore, ct)
                return! tcs.Task |> Async.AwaitTaskCorrect
        }

        /// <summary>
        ///     Asynchronously await an IObservable to yield results.
        ///     Returns the first value to be observed.
        /// </summary>
        /// <param name="observable">Observable source.</param>
        /// <param name="timeout">Timeout in milliseconds. Defaults to no timeout.</param>
        static member AwaitObservable(observable: IObservable<'T>, ?timeoutMilliseconds:int) : Async<'T> =
            let tcs = new TaskCompletionSource<'T>()
            let rec observer t = tcs.TrySetResult t |> ignore ; remover.Dispose()
            and remover : IDisposable = observable.Subscribe observer

            match timeoutMilliseconds with
            | None -> tcs.Task |> Async.AwaitTaskCorrect
            | Some t -> Async.WithTimeout(tcs.Task |> Async.AwaitTaskCorrect, t)