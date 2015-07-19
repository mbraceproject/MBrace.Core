namespace MBrace.Runtime.InMemoryRuntime

open System
open System.Management
open System.Threading
open System.Threading.Tasks

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils

/// Specifies memory semantics in InMemory MBrace execution
type MemoryEmulation =
    /// Freely share values across MBrace workflows; async semantics.
    | Shared                = 0
    /// Freely share values across MBrace workflows but
    /// emulate serialization errors by checking that they are serializable.
    | EnsureSerializable    = 1
    /// Values copied when passed across worfklows; full distributed semantics.
    | Copied                = 2

[<NoEquality; NoComparison; AutoSerializable(false)>]
type private EmulatedValue<'T> =
    | Shared of 'T
    | Cloned of 'T
with
    member inline ev.Value =
        match ev with
        | Shared t -> t
        | Cloned t -> FsPickler.Clone t

    member inline ev.RawValue =
        match ev with
        | Shared t -> t
        | Cloned t -> t

    member inline ev.Cast<'S> () =
        let inline cast(t : 'T) =
            match box t with
            | null as n -> n :?> 'S
            | :? 'S as s -> s
            | _ -> raise <| new InvalidCastException()

        match ev with 
        | Shared t -> Shared (cast t)
        | Cloned t -> Cloned (cast t)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module private MemoryEmulation =

    let isShared (mode : MemoryEmulation) =
        match mode with
        | MemoryEmulation.EnsureSerializable
        | MemoryEmulation.Copied -> false
        | _ -> true

module private EmulatedValue =

    /// Performs cloning of value based on emulation semantics
    let clone (mode : MemoryEmulation) (value : 'T) : 'T =
        match mode with
        | MemoryEmulation.Copied -> 
            FsPickler.Clone value

        | MemoryEmulation.EnsureSerializable ->
            FsPickler.EnsureSerializable(value, failOnCloneableOnlyTypes = false)
            value

        | MemoryEmulation.Shared 
        | _ -> value
    
    /// Creates a copy of provided value given emulation semantics
    let create (mode : MemoryEmulation) shareCloned (value : 'T) =
        match mode with
        | MemoryEmulation.Copied -> 
            let clonedVal = FsPickler.Clone value
            if shareCloned then Shared clonedVal
            else Cloned clonedVal

        | MemoryEmulation.EnsureSerializable ->
            FsPickler.EnsureSerializable(value, failOnCloneableOnlyTypes = false)
            Shared value

        | MemoryEmulation.Shared 
        | _ -> Shared value


/// Cloud cancellation token implementation that wraps around System.Threading.CancellationToken
[<AutoSerializable(false); CloneableOnly>]
type InMemoryCancellationToken(token : CancellationToken) =
    new () = new InMemoryCancellationToken(new CancellationToken())
    /// Returns true if cancellation token has been canceled
    member __.IsCancellationRequested = token.IsCancellationRequested
    /// Local System.Threading.CancellationToken instance
    member __.LocalToken = token
    interface ICloudCancellationToken with
        member __.IsCancellationRequested = token.IsCancellationRequested
        member __.LocalToken = token

/// Cloud cancellation token source implementation that wraps around System.Threading.CancellationTokenSource
[<AutoSerializable(false); CloneableOnly>]
type InMemoryCancellationTokenSource (cts : CancellationTokenSource) =
    let token = new InMemoryCancellationToken (cts.Token)
    new () = new InMemoryCancellationTokenSource(new CancellationTokenSource())
    /// InMemoryCancellationToken instance
    member __.Token = token
    /// Trigger cancelation for the cts
    member __.Cancel() = cts.Cancel()
    /// Local System.Threading.CancellationTokenSource instance
    member __.LocalCancellationTokenSource = cts
    interface ICloudCancellationTokenSource with
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

        new InMemoryCancellationTokenSource(lcts)

/// In-Memory WorkerRef implementation
[<AutoSerializable(false); CloneableOnly>]
type InMemoryWorker private () =
    static let singleton = new InMemoryWorker()
    let name = System.Net.Dns.GetHostName()
    let pid = System.Diagnostics.Process.GetCurrentProcess().Id
    // TODO : move to perfmon and make mono compatible
    let cpuClockSpeed =
        if not runsOnMono then
            use searcher = new ManagementObjectSearcher("SELECT MaxClockSpeed FROM Win32_Processor")
            use qObj = searcher.Get() 
                        |> Seq.cast<ManagementBaseObject> 
                        |> Seq.exactlyOne

            let cpuFreq = qObj.["MaxClockSpeed"] :?> uint32
            Some <| float cpuFreq
        else
            None

    interface IWorkerRef with
        member __.Hostname = name
        member __.Type = "InMemory worker"
        member __.Id = name
        member __.ProcessorCount = Environment.ProcessorCount
        member __.MaxCpuClock = 
            match cpuClockSpeed with
            | Some cpu -> cpu
            | None -> raise <| NotImplementedException("Mono not supporting CPU clock speed.")

        member __.ProcessId = pid
        member __.CompareTo(other : obj) =
            match other with
            | :? InMemoryWorker -> 0
            | :? IWorkerRef as wr -> compare name wr.Id
            | _ -> invalidArg "other" "invalid comparand."

    /// Gets a WorkerRef instance that corresponds to the instance
    static member LocalInstance : InMemoryWorker = singleton

/// Cloud task implementation that wraps around System.Threading.Task for inmemory runtimes
[<AutoSerializable(false); CloneableOnly>]
type InMemoryTask<'T> internal (task : Task<'T>, ct : ICloudCancellationToken) =
    member __.LocalTask = task
    interface ICloudTask<'T> with
        member __.Id = sprintf ".NET task %d" task.Id
        member __.AwaitResult(?timeoutMilliseconds:int) = async {
            try return! Async.WithTimeout(Async.AwaitTaskCorrect task, ?timeoutMilliseconds = timeoutMilliseconds)
            with :? AggregateException as e -> return! Async.Raise (e.InnerExceptions.[0])
        }

        member __.TryGetResult () = async { return task.TryGetResult() }
        member __.Status = task.Status
        member __.IsCompleted = task.IsCompleted
        member __.IsFaulted = task.IsFaulted
        member __.IsCanceled = task.IsCanceled
        member __.CancellationToken = ct
        member __.Result = task.GetResult()


/// Cloud task implementation that wraps around System.Threading.TaskCompletionSource for inmemory runtimes
[<AutoSerializable(false); CloneableOnly>]
type private InMemoryTaskCompletionSource<'T> (?cancellationToken : ICloudCancellationToken) =
    let tcs = new TaskCompletionSource<'T>()
    let cts =
        match cancellationToken with
        | None -> InMemoryCancellationTokenSource()
        | Some ct -> InMemoryCancellationTokenSource.CreateLinkedCancellationTokenSource [|ct|]

    let task = new InMemoryTask<'T>(tcs.Task, cts.Token)

    member __.CancellationTokenSource = cts
    member __.LocalTaskCompletionSource = tcs
    member __.Task = task