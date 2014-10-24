namespace Nessos.MBrace.SampleRuntime

    open System
    open System.IO
    open System.Diagnostics
    open System.Threading

    open Nessos.MBrace
    open Nessos.MBrace.SampleRuntime.Scheduler

    module internal Argument =
        let ofRuntime (runtime : RuntimeState) =
            let pickle = Vagrant.pickler.Pickle(runtime)
            System.Convert.ToBase64String pickle

        let toRuntime (args : string []) =
            let bytes = System.Convert.FromBase64String(args.[0])
            Vagrant.pickler.UnPickle<RuntimeState> bytes

    type MBraceRuntime private (workerCount : int) =

        static let mutable exe = None
            
        let state = RuntimeState.InitLocal()

        let initProc _ =
            let exe = MBraceRuntime.WorkerExecutable
            let args = Argument.ofRuntime state
            let psi = new ProcessStartInfo(exe, args)
            psi.WorkingDirectory <- Path.GetDirectoryName exe
            psi.UseShellExecute <- true
            Process.Start psi

        let procs = Array.init workerCount initProc
        
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

        member __.GetCancellationTokenSource(?parent) = 
            state.CancellationTokenManager.RequestCancellationTokenSource(?parent = parent)

        static member InitLocal(workerCount : int) = 
            if workerCount < 1 then invalidArg "workerCount" "must be positive."
            new MBraceRuntime(workerCount)

        static member WorkerExecutable
            with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
            and set path = 
                let path = Path.GetFullPath path
                if File.Exists path then exe <- Some path
                else raise <| FileNotFoundException(path)