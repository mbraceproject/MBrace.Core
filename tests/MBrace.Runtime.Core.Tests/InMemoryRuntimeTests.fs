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
type ``ThreadPool Parallelism Tests`` (memoryMode : MemoryEmulation) =
    inherit ``Distribution Tests``(parallelismFactor = 100, delayFactor = 1000)

    let logger = InMemoryLogTester()
    let imem = InMemoryRuntime.Create(logger = logger, memoryMode = memoryMode)

    override __.RunRemote(workflow : Cloud<'T>) = Choice.protect (fun () -> imem.Run(workflow))
    override __.RunRemote(workflow : ICloudCancellationTokenSource -> #Cloud<'T>) =
        let cts = imem.CreateCancellationTokenSource()
        Choice.protect(fun () ->
            imem.Run(workflow cts, cancellationToken = cts.Token))

    override __.RunLocally(workflow : Cloud<'T>) = imem.Run(workflow)
    override __.IsTargetWorkerSupported = false
    override __.Logs = logger :> _
    override __.FsCheckMaxTests = 100
    override __.UsesSerialization = memoryMode <> MemoryEmulation.Shared
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif


type ``ThreadPool Parallelism Tests (Shared)`` () =
    inherit ``ThreadPool Parallelism Tests`` (MemoryEmulation.Shared)

type ``ThreadPool Parallelism Tests (EnsureSerializable)`` () =
    inherit ``ThreadPool Parallelism Tests`` (MemoryEmulation.EnsureSerializable)

type ``ThreadPool Parallelism Tests (Copied)`` () =
    inherit ``ThreadPool Parallelism Tests`` (MemoryEmulation.Copied)

type ``InMemory CloudValue Tests`` () =
    inherit ``CloudValue Tests`` (parallelismFactor = 100)

    let valueProvider = MBrace.Runtime.InMemoryRuntime.InMemoryValueProvider() :> ICloudValueProvider
    let imem = InMemoryRuntime.Create(memoryMode = MemoryEmulation.Shared, valueProvider = valueProvider)

    override __.IsSupportedLevel lvl = valueProvider.IsSupportedStorageLevel lvl

    override __.RunRemote(workflow) = imem.Run workflow
    override __.RunLocally(workflow) = imem.Run workflow

type ``InMemory CloudAtom Tests`` () =
    inherit ``CloudAtom Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryMode = MemoryEmulation.Shared)

    override __.RunRemote(workflow) = imem.Run workflow
    override __.RunLocally(workflow) = imem.Run workflow
#if DEBUG
    override __.Repeats = 10
#else
    override __.Repeats = 3
#endif

type ``InMemory CloudQueue Tests`` () =
    inherit ``CloudQueue Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryMode = MemoryEmulation.Shared)

    override __.RunRemote(workflow) = imem.Run workflow
    override __.RunLocally(workflow) = imem.Run workflow

type ``InMemory CloudDictionary Tests`` () =
    inherit ``CloudDictionary Tests`` (parallelismFactor = 100)

    let imem = InMemoryRuntime.Create(memoryMode = MemoryEmulation.Shared)

    override __.IsInMemoryFixture = true
    override __.RunRemote(workflow) = imem.Run workflow
    override __.RunLocally(workflow) = imem.Run workflow

type ``InMemory CloudFlow tests`` () =
    inherit ``CloudFlow tests`` ()

    let imem = InMemoryRuntime.Create(fileConfig = Config.fsConfig, serializer = Config.serializer, memoryMode = MemoryEmulation.Shared)

    override __.RunRemote(workflow : Cloud<'T>) = imem.Run workflow
    override __.RunLocally(workflow : Cloud<'T>) = imem.Run workflow
    override __.FsCheckMaxNumberOfTests = if isAppVeyorInstance then 20 else 100
    override __.FsCheckMaxNumberOfIOBoundTests = if isAppVeyorInstance then 5 else 30