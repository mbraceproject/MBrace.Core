namespace MBrace.SampleRuntime.Tests

open System.IO

open NUnit.Framework

open MBrace
open MBrace.Streams.Tests
open MBrace.SampleRuntime

[<Category("CloudStreams.Cluster")>]
type ``SampleRuntime Streams Tests`` () =
    inherit ``CloudStreams tests`` ()
        
    let mutable currentRuntime : MBraceRuntime option = None
      
    override __.FsCheckMaxNumberOfTests = 10  
    override __.Run(expr : Cloud<'T>) : 'T = currentRuntime.Value.Run(expr, faultPolicy = FaultPolicy.NoRetry)
    override __.RunLocal(expr : Cloud<'T>) : 'T = currentRuntime.Value.RunLocal(expr)

    [<TestFixtureSetUp>]
    member __.InitRuntime() =
        match currentRuntime with
        | Some runtime -> runtime.KillAllWorkers()
        | None -> ()
            
        MBraceRuntime.WorkerExecutable <- Path.Combine(__SOURCE_DIRECTORY__, "../../bin/MBrace.SampleRuntime.exe")
        let runtime = MBraceRuntime.InitLocal(4)
        currentRuntime <- Some runtime

    [<TestFixtureTearDown>]
    member __.FiniRuntime() =
        match currentRuntime with
        | None -> invalidOp "No runtime specified in test fixture."
        | Some r -> r.KillAllWorkers() ; currentRuntime <- None