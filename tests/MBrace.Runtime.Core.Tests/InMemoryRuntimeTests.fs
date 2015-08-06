namespace MBrace.Runtime.Tests

open System.Threading
open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime.InMemoryRuntime

type InMemoryLogTester () =
    let logs = new ResizeArray<string>()

    interface ILogTester with
        member __.GetLogs () = logs.ToArray()
        member __.Clear () = lock logs (fun () -> logs.Clear())

    interface ICloudLogger with
        member __.Log msg = lock logs (fun () -> logs.Add msg)

[<AbstractClass>]
type ``ThreadPool Cloud Tests`` (memoryEmulation : MemoryEmulation) =
    inherit ``Cloud Tests``(parallelismFactor = 100, delayFactor = 1000)

    let logger = InMemoryLogTester()
    let imem = InMemoryRuntime.Create(logger = logger, memoryEmulation = memoryEmulation)

    member __.Runtime = imem

    override __.RunOnCloud(workflow : Cloud<'T>) = Choice.protect (fun () -> imem.Run(workflow))
    override __.RunOnCloud(workflow : ICloudCancellationTokenSource -> #Cloud<'T>) =
        let cts = imem.CreateCancellationTokenSource()
        Choice.protect(fun () ->
            imem.Run(workflow cts, cancellationToken = cts.Token))

    override __.RunOnCurrentMachine(workflow : Cloud<'T>) = imem.Run(workflow)
    override __.IsTargetWorkerSupported = false
    override __.IsSiftedWorkflowSupported = false
    override __.Logs = logger :> _
    override __.FsCheckMaxTests = 100
    override __.UsesSerialization = memoryEmulation <> MemoryEmulation.Shared
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif


type ``ThreadPool Cloud Tests (Shared)`` () =
    inherit ``ThreadPool Cloud Tests`` (MemoryEmulation.Shared)

    member __.``Memory Semantics`` () =
        cloud {
            let cell = ref 0
            let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { ignore <| Interlocked.Increment cell } ]
            return !cell
        } |> base.Runtime.Run |> shouldEqual 10

    member __.``Serialization Semantics`` () =
        cloud {
            return box(new System.IO.MemoryStream())
        }|> base.Runtime.Run |> ignore
        

type ``ThreadPool Cloud Tests (EnsureSerializable)`` () =
    inherit ``ThreadPool Cloud Tests`` (MemoryEmulation.EnsureSerializable)

    member __.``Memory Semantics`` () =
        cloud {
            let cell = ref 0
            let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { ignore <| Interlocked.Increment cell } ]
            return !cell
        } |> base.Runtime.Run |> shouldEqual 10

type ``ThreadPool Cloud Tests (Copied)`` () =
    inherit ``ThreadPool Cloud Tests`` (MemoryEmulation.Copied)

    member __.``Memory Semantics`` () =
        cloud {
            let cell = ref 0
            let! results = Cloud.Parallel [ for i in 1 .. 10 -> cloud { ignore <| Interlocked.Increment cell } ]
            return !cell
        } |> base.Runtime.Run |> shouldEqual 0

type ``InMemory CloudValue Tests`` () =
    inherit ``CloudValue Tests`` (parallelismFactor = 100)

    let valueProvider = MBrace.Runtime.InMemoryRuntime.InMemoryValueProvider() :> ICloudValueProvider
    let imem = InMemoryRuntime.Create(memoryEmulation = MemoryEmulation.Shared, valueProvider = valueProvider)

    override __.IsSupportedLevel lvl = lvl = StorageLevel.Memory || lvl = StorageLevel.MemorySerialized

    override __.RunOnCloud(workflow) = imem.Run workflow
    override __.RunOnCurrentMachine(workflow) = imem.Run workflow

type ``InMemory CloudAtom Tests`` () =
    inherit ``CloudAtom Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryEmulation = MemoryEmulation.EnsureSerializable)

    override __.RunOnCloud(workflow) = imem.Run workflow
    override __.RunOnCurrentMachine(workflow) = imem.Run workflow
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

type ``InMemory CloudQueue Tests`` () =
    inherit ``CloudQueue Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryEmulation = MemoryEmulation.EnsureSerializable)

    override __.RunOnCloud(workflow) = imem.Run workflow
    override __.RunOnCurrentMachine(workflow) = imem.Run workflow

type ``InMemory CloudDictionary Tests`` () =
    inherit ``CloudDictionary Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryEmulation = MemoryEmulation.EnsureSerializable)

    override __.IsInMemoryFixture = true
    override __.RunOnCloud(workflow) = imem.Run workflow
    override __.RunOnCurrentMachine(workflow) = imem.Run workflow

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    let imem = InMemoryRuntime.Create(fileConfig = Config.fsConfig, serializer = Config.serializer, memoryEmulation = MemoryEmulation.Copied)

    override __.RunOnCloud(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunOnCurrentMachine(workflow : Cloud<'T>) = imem.Run workflow
    override __.IsSupportedStorageLevel(level : StorageLevel) = level.HasFlag StorageLevel.Memory || level.HasFlag StorageLevel.MemorySerialized
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30