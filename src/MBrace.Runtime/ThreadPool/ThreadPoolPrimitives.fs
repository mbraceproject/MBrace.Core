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
type ThreadPoolTask<'T> internal (task : Task<'T>, ct : ICloudCancellationToken) =
    member __.LocalTask = task
    interface ICloudProcess<'T> with
        member __.Id = sprintf "System.Threading.Task %d" task.Id
        member __.AwaitResult(?timeoutMilliseconds:int) = async {
            try return! Async.WithTimeout(Async.AwaitTaskCorrect task, ?timeoutMilliseconds = timeoutMilliseconds)
            with :? AggregateException as e -> return! Async.Raise (e.InnerExceptions.[0])
        }

        member __.AwaitResultBoxed(?timeoutMilliseconds:int) : Async<obj> = async {
            try 
                let! r = Async.WithTimeout(Async.AwaitTaskCorrect task, ?timeoutMilliseconds = timeoutMilliseconds)
                return r :> obj

            with :? AggregateException as e -> 
                return! Async.Raise (e.InnerExceptions.[0])
        }

        member __.TryGetResult () = async { return task.TryGetResult() }
        member __.TryGetResultBoxed () = async { return task.TryGetResult() |> Option.map box }
        member __.Status = task.Status
        member __.IsCompleted = task.IsCompleted
        member __.IsFaulted = task.IsFaulted
        member __.IsCanceled = task.IsCanceled
        member __.CancellationToken = ct
        member __.Result = task.GetResult()
        member __.ResultBoxed = task.GetResult() :> obj


/// FsPickler Binary serializer for use by thread pool runtimes
type ThreadPoolFsPicklerBinarySerializer() =
    inherit FsPicklerStoreSerializer ()
    override __.Id = "In-Memory FsPickler Binary Serializer"
    override __.CreateSerializer () = FsPickler.CreateBinarySerializer() :> _

/// FsPickler Xml serializer for use by thread pool runtimes
type ThreadPoolFsPicklerXmlSerializer() =
    inherit FsPicklerStoreSerializer ()
    override __.Id = "In-Memory FsPickler Xml Serializer"
    override __.CreateSerializer () = FsPickler.CreateXmlSerializer() :> _

/// FsPickler Json serializer for use by thread pool runtimes
type ThreadPoolFsPicklerJsonSerializer() =
    inherit FsPicklerStoreSerializer ()
    override __.Id = "In-Memory FsPickler Json Serializer"
    override __.CreateSerializer () = FsPickler.CreateJsonSerializer() :> _