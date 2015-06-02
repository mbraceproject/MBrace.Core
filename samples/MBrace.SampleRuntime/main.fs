module internal MBrace.SampleRuntime.Main

open Nessos.Thespian
open Nessos.Thespian.Remote.Protocols
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.SampleRuntime.Actors
open MBrace.SampleRuntime.Worker

let maxConcurrentTasks = 10

//[<EntryPoint>]
//let main (args : string []) =
//    try
//        do Config.Init()
//        let logger = new ConsoleLogger()
//        Config.Logger <- logger
//
//        let evaluator =
//#if APPDOMAIN_ISOLATION
//            new AppDomainJobEvaluator() :> IJobEvaluator
//#else
//            new LocalJobEvaluator() :> IJobEvaluator
//#endif
//        if args.Length > 0 then
//            let runtime = Argument.toRuntime args
//            Async.RunSync (Worker.initWorker runtime logger maxConcurrentTasks evaluator)
//            0
//        else
//            Actor.Stateful (new System.Threading.CancellationTokenSource()) (Worker.workerManager logger evaluator)
//            |> Actor.rename "workerManager"
//            |> Actor.publish [ Protocols.utcp() ]
//            |> Actor.start
//            |> ignore
//            
//            while true do System.Threading.Thread.Sleep 10000
//            0
//    with e ->
//        printfn "Unhandled exception : %O" e
//        let _ = System.Console.ReadKey()
//        1
