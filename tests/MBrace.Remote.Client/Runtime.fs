namespace Nessos.MBrace.Remote

    open System
    open System.IO
    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace
    open Nessos.UnionArgParser
    open Nessos.MBrace.Remote.Actors
    open Nessos.MBrace.Remote.Scheduler

    [<NoAppSettings>]
    type internal Argument =
        | [<Mandatory>] Pickled_Runtime of byte []
    with
        interface IArgParserTemplate with
            member __.Usage = ""

        static member Parser = UnionArgParser.Create<Argument> ()

    type MBraceRuntime private (workers : int) =

        static let argParser = Argument.Parser
        static let mutable exe = None
            
        let state = RuntimeState.InitLocal()

        let initProc () =
            let exe = MBraceRuntime.WorkerExecutable
            let args = [ Pickled_Runtime (Vagrant.vagrant.Pickler.Pickle state) ] |> argParser.PrintCommandLineFlat
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Process.Start psi |> ignore

        do for i = 1 to workers do initProc ()
        
        member __.RunAsync(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) = async {
            let cts = state.CancellationTokenManager.RequestCancellationTokenSource()
            cancellationToken |> Option.iter (fun ct -> ct.Register(fun () -> cts.Cancel()) |> ignore)
            let resultCell = state.StartAsCell cts workflow
            let! result = resultCell.AwaitResult()
            return result.Value
        }

        member __.Run(workflow : Cloud<'T>, ?cancellationToken : CancellationToken) =
            __.RunAsync(workflow, ?cancellationToken = cancellationToken) |> Async.RunSynchronously

        static member InitLocal(workers : int) = new MBraceRuntime(workers)

        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)
                    