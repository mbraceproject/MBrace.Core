[<AutoOpen>]
module MBrace.Core.Internals.AsyncExtensions

open System
open System.Threading
open System.Threading.Tasks

type Task with
    /// Gets the inner exception of the faulted task.
    member t.InnerException =
        let e = t.Exception
        if e.InnerExceptions.Count = 1 then e.InnerExceptions.[0]
        else
            e :> exn

type Task<'T> with

    /// <summary>
    ///     Returns Some result if completed, None if pending, exception if faulted.
    /// </summary>
    member t.TryGetResult() : 'T option =
        match t.Status with
        | TaskStatus.RanToCompletion -> Some t.Result
        | TaskStatus.Faulted -> ExceptionDispatchInfo.raiseWithCurrentStackTrace true t.InnerException
        | TaskStatus.Canceled -> raise <| new OperationCanceledException()
        | _ -> None

    /// Gets the result of given task so that in the event of exception
    /// the actual user exception is raised as opposed to being wrapped
    /// in a System.AggregateException
    member t.CorrectResult : 'T =
        try t.Result
        with :? AggregateException as ae when ae.InnerExceptions.Count = 1 ->
            ExceptionDispatchInfo.raiseWithCurrentStackTrace true ae.InnerExceptions.[0]

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
            let _ = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
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
            let _ = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
            ()

        tcs.Task

type Async =

    /// <summary>
    ///     Efficiently reraise exception, without losing its existing stacktrace.
    /// </summary>
    /// <param name="e"></param>
    static member Raise<'T> (e : exn) : Async<'T> = Async.FromContinuations(fun (_,ec,_) -> ec e)

    /// <summary>
    ///     Runs the asynchronous computation and awaits its result.
    ///     Preserves original stacktrace for any exception raised.
    /// </summary>
    /// <param name="workflow">Workflow to be run.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    static member RunSync(workflow : Async<'T>, ?cancellationToken : CancellationToken) =
        let tcs = new TaskCompletionSource<Choice<'T,exn,OperationCanceledException>>()
        let inline commit f r = ignore <| tcs.TrySetResult(f r)
        Trampoline.QueueWorkItem(fun () ->
            Async.StartWithContinuations(workflow, 
                commit Choice1Of3, commit Choice2Of3, commit Choice3Of3, 
                ?cancellationToken = cancellationToken))

        match tcs.Task.Result with
        | Choice1Of3 t -> t
        | Choice2Of3 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e
        | Choice3Of3 e -> ExceptionDispatchInfo.raiseWithCurrentStackTrace false e

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    static member AwaitTaskCorrect(task : Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,cc) ->
            task.ContinueWith(fun (t : Task<'T>) -> 
                if task.IsFaulted then ec t.InnerException 
                elif task.IsCanceled then cc(new OperationCanceledException())
                else sc t.Result)
            |> ignore)

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    static member AwaitTaskCorrect(task : Task) : Async<unit> =
        Async.FromContinuations(fun (sc,ec,cc) ->
            task.ContinueWith(fun (t : Task) -> 
                if task.IsFaulted then ec t.InnerException 
                elif task.IsCanceled then cc(new OperationCanceledException())
                else sc ())
            |> ignore)
        
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

/// AsyncBuilder Task extensions
type AsyncBuilder with
    member inline b.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = b.Bind(Async.AwaitTaskCorrect f, g)
    member inline b.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> = b.Bind(Async.AwaitTaskCorrect f, g)
    member inline b.ReturnFrom(f : Task<'T>) : Async<'T> = b.ReturnFrom(Async.AwaitTaskCorrect f)
    member inline b.ReturnFrom(f : Task) : Async<unit> = b.ReturnFrom(Async.AwaitTaskCorrect f)