namespace MBrace.ThreadPool

open System
open System.IO
open System.Threading
open System.Threading.Tasks

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals

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

/// Cloud task implementation that wraps around System.Threading.Task for inmemory runtimes
[<AutoSerializable(false); CloneableOnly>]
type ThreadPoolTask<'T> internal (task : Task<'T>, ct : ICloudCancellationToken) =
    member __.LocalTask = task
    interface ICloudTask<'T> with
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


[<AutoSerializable(false); CloneableOnly>]
type ThreadPoolFsPicklerSerializer(serializer : FsPicklerSerializer) =
    interface ISerializer with
        member __.Id = serializer.PickleFormat
        member __.IsSerializable(value : 'T) = try FsPickler.EnsureSerializable value ; true with _ -> false
        member __.Serialize (target : Stream, value : 'T, leaveOpen : bool) = serializer.Serialize(target, value, leaveOpen = leaveOpen)
        member __.Deserialize<'T>(stream, leaveOpen) = serializer.Deserialize<'T>(stream, leaveOpen = leaveOpen)
        member __.SeqSerialize(stream, values : 'T seq, leaveOpen) = serializer.SerializeSequence(stream, values, leaveOpen = leaveOpen)
        member __.SeqDeserialize<'T>(stream, leaveOpen) = serializer.DeserializeSequence<'T>(stream, leaveOpen = leaveOpen)
        member __.ComputeObjectSize<'T>(graph:'T) = serializer.ComputeSize graph
        member __.Clone(graph:'T) = FsPickler.Clone graph

    static member CreateBinarySerializer() = new ThreadPoolFsPicklerSerializer(FsPickler.CreateBinarySerializer())
    static member CreateJsonSerializer() = new ThreadPoolFsPicklerSerializer(FsPickler.CreateBinarySerializer())
    static member CreateXmlSerializer() = new ThreadPoolFsPicklerSerializer(FsPickler.CreateBinarySerializer())