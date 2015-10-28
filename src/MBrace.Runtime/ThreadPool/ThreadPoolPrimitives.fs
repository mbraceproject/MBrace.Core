namespace MBrace.ThreadPool

open System
open System.IO
open System.Threading
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.FsPickler.Json

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils

/// Cloud cancellation token implementation that wraps around System.Threading.CancellationToken
[<AutoSerializable(false); CloneableOnly>]
type ThreadPoolCancellationToken(token : CancellationToken) =
    new () = new ThreadPoolCancellationToken(new CancellationToken())
    /// Returns true if cancellation token has been canceled
    member __.IsCancellationRequested = token.IsCancellationRequested
    /// Local System.Threading.CancellationToken instance
    member __.LocalToken = token
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested
        member __.LocalToken = token

/// Cloud cancellation token source implementation that wraps around System.Threading.CancellationTokenSource
[<AutoSerializable(false); CloneableOnly>]
type ThreadPoolCancellationTokenSource (cts : CancellationTokenSource) =
    let token = new ThreadPoolCancellationToken (cts.Token)
    new () = new ThreadPoolCancellationTokenSource(new CancellationTokenSource())
    /// InMemoryCancellationToken instance
    member __.Token = token
    /// Trigger cancelation for the cts
    member __.Cancel() = cts.Cancel()
    /// Local System.Threading.CancellationTokenSource instance
    member __.LocalCancellationTokenSource = cts
    interface ICloudCancellationTokenSource with
        member __.Dispose() = async {cts.Cancel()}
        member __.Cancel() = cts.Cancel()
        member __.Token = token :> _

    /// <summary>
    ///     Creates a local linked cancellation token source from provided parent tokens
    /// </summary>
    /// <param name="parents">Parent cancellation tokens.</param>
    static member CreateLinkedCancellationTokenSource(parents : ICloudCancellationToken []) =
        let ltokens = parents |> Array.map (fun t -> t.LocalToken)
        let lcts =
            if Array.isEmpty ltokens then new CancellationTokenSource()
            else
                CancellationTokenSource.CreateLinkedTokenSource ltokens

        new ThreadPoolCancellationTokenSource(lcts)

/// CloudLogger implementation that writes output to stdout
[<AutoSerializable(false); CloneableOnly>]
type ConsoleCloudLogger() =
    interface ICloudLogger with
        member __.Log msg = System.Console.WriteLine msg

/// Cloud process implementation that wraps around System.Threading.Task for inmemory runtimes
[<AutoSerializable(false); CloneableOnly>]
type ThreadPoolProcess<'T> internal (task : Task<'T>, ct : ICloudCancellationToken) =
    member __.LocalTask = task
    interface ICloudProcess<'T> with
        member __.Id = sprintf "System.Threading.Task %d" task.Id
        member __.WaitAsync(?timeoutMilliseconds:int) =
            Async.WithTimeout(Async.AwaitTaskCorrect task |> Async.Ignore, ?timeoutMilliseconds = timeoutMilliseconds)
        member __.AwaitResultAsync(?timeoutMilliseconds:int) =
            Async.WithTimeout(Async.AwaitTaskCorrect task, ?timeoutMilliseconds = timeoutMilliseconds)

        member __.AwaitResultBoxedAsync(?timeoutMilliseconds:int) : Async<obj> = async {
            let! r = Async.WithTimeout(Async.AwaitTaskCorrect task, ?timeoutMilliseconds = timeoutMilliseconds)
            return r :> obj
        }

        member __.TryGetResultAsync () = async { return task.TryGetResult() }
        member __.TryGetResultBoxedAsync () = async { return task.TryGetResult() |> Option.map box }
        member __.Status = 
            match task.Status with
            | TaskStatus.Created -> CloudProcessStatus.Created
            | TaskStatus.Faulted -> CloudProcessStatus.UserException
            | TaskStatus.RanToCompletion -> CloudProcessStatus.Completed
            | TaskStatus.Running -> CloudProcessStatus.Running
            | TaskStatus.WaitingForActivation -> CloudProcessStatus.WaitingToRun
            | TaskStatus.WaitingForChildrenToComplete -> CloudProcessStatus.Running
            | TaskStatus.WaitingToRun -> CloudProcessStatus.WaitingToRun
            | _ -> enum -1

        member __.IsCompleted = task.IsCompleted
        member __.IsFaulted = task.IsFaulted
        member __.IsCanceled = task.IsCanceled
        member __.CancellationToken = ct
        member __.Result = task.CorrectResult
        member __.ResultBoxed = task.CorrectResult :> obj