namespace MBrace.Tests

open System.Threading

open MBrace
open MBrace.Continuation
open MBrace.InMemory

open NUnit.Framework

type InMemoryLogger () =
    let logs = new ResizeArray<string>()

    member __.Logs = logs.ToArray()
    member __.Clear () = lock logs (fun () -> logs.Clear())

    interface ICloudLogger with
        member __.Log msg = lock logs (fun () -> logs.Add msg)

type InMemoryCancellationTokenSource() =
    let cts = new CancellationTokenSource()
    member __.Token = cts.Token
    interface ICancellationTokenSource with
        member __.Cancel() = cts.Cancel()

type ``ThreadPool Parallelism Tests`` () =
    inherit ``Parallelism Tests``(100)

    let logger = InMemoryLogger()
    let imem = InMemoryRuntime.Create(logger = logger)

    override __.Run(workflow : Cloud<'T>) = Choice.protect (fun () -> imem.Run(workflow))
    override __.Run(workflow : ICancellationTokenSource -> Cloud<'T>) =
        let cts = new InMemoryCancellationTokenSource()
        Choice.protect(fun () ->
            imem.Run(workflow cts, cancellationToken = cts.Token))

    override __.RunLocal(workflow : Cloud<'T>) = imem.Run(workflow)
    override __.IsTargetWorkerSupported = false

    [<Test>]
    member __.``InMemory Logging`` () =
        logger.Clear()
        cloud {
            for i in 1 .. 100 do
                do! Cloud.Logf "message %d" i
        } |> __.Run |> ignore
        
        logger.Logs.Length |> shouldEqual 100


type ``InMemory CloudAtom Tests`` () =
    inherit ``CloudAtom Tests`` (nParallel = 100)

    let imem = InMemoryRuntime.Create()

    override __.Run(workflow) = imem.Run workflow
    override __.RunLocal(workflow) = imem.Run workflow
    override __.AtomClient = imem.StoreClient.CloudAtom

type ``InMemory CloudChannel Tests`` () =
    inherit ``CloudChannel Tests`` ()

    let imem = InMemoryRuntime.Create()

    override __.Run(workflow) = imem.Run workflow
    override __.RunLocal(workflow) = imem.Run workflow
    override __.ChannelClient = imem.StoreClient.CloudChannel