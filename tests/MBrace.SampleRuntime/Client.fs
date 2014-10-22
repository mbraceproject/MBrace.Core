namespace Nessos.MBrace.SampleRuntime

    open System
    open System.IO
    open System.Diagnostics
    open System.Threading

    open Nessos.UnionArgParser

    open Nessos.MBrace
    open Nessos.MBrace.SampleRuntime.PortablePickle
    open Nessos.MBrace.SampleRuntime.Scheduler

    [<NoAppSettings>]
    type internal Argument =
        | [<Mandatory>] Pickled_Runtime of byte []
    with
        interface IArgParserTemplate with
            member __.Usage = ""

        static member Parser = UnionArgParser.Create<Argument> ()
        static member OfRuntime(runtime : RuntimeState) =
            let pickle = PortablePickle.Pickle(runtime, includeAssemblies = false)
            Pickled_Runtime pickle.Pickle

        static member ToRuntime(Pickled_Runtime pickle) =
            PortablePickle.UnPickle<RuntimeState>({ Pickle = pickle ; Dependencies = []})

    type MBraceRuntime private (workers : int) =

        static let argParser = Argument.Parser
        static let mutable exe = None
            
        let state = RuntimeState.InitLocal()

        let initProc _ =
            let exe = MBraceRuntime.WorkerExecutable
            let args = argParser.PrintCommandLineFlat [ Argument.OfRuntime state ]
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Process.Start psi

        let procs = Array.init workers initProc
        
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) = async {
            let cts = state.CancellationTokenManager.RequestCancellationTokenSource()
            cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
            let resultCell = state.StartAsCell cts workflow
            let! result = resultCell.AwaitResult()
            return result.Value
        }

        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken) |> Async.RunSynchronously

        member __.Kill () = for p in procs do try p.Kill() with _ -> ()

        static member InitLocal(workers : int) = new MBraceRuntime(workers)

        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)