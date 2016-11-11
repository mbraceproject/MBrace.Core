namespace MBrace.Runtime.Components

open System
open System.Diagnostics
open MBrace.Runtime
open Microsoft.FSharp.Control

[<NoEquality; NoComparison>]
type private ProcessMonitorMsg =
    | Stop of AsyncReplyChannel<exn option>
    | Start of AsyncReplyChannel<exn option>
    | GetState of AsyncReplyChannel<Process option>
    | Recover

type ProcessMonitorRetryPolicy =
    abstract ShouldRetryAfter : lastProcDurations:TimeSpan list -> TimeSpan option

/// Provides class for monitoring child processes;
/// If the child process dies then it will be restarted by the parent
[<AutoSerializable(false)>]
type ProcessMonitor private (executable : string, arguments : string, workingDirectory : string, 
                                retryPolicy : ProcessMonitorRetryPolicy, logger : ISystemLogger) =

    static let errorf fmt = Printf.ksprintf (InvalidOperationException >> unbox<exn>) fmt
    static let getDuration(proc : Process) = let d = (proc.StartTime - DateTime.Now) in d.Duration()

    let startProcess () =
        try
            let proc = new Process()
            let psi = proc.StartInfo
            psi.FileName <- executable
            psi.Arguments <- arguments
            psi.WorkingDirectory <- workingDirectory
            psi.UseShellExecute <- false
            psi.CreateNoWindow <- true
            proc.EnableRaisingEvents <- true
            if not <| proc.Start() then
                Choice2Of2 <| errorf "Failed to start process '%s'" executable
            else
                Choice1Of2 proc
        with e -> Choice2Of2 e

    let rec behaviour (state : (TimeSpan list * Process) option) (self : MailboxProcessor<ProcessMonitorMsg>) = async {
        let! msg = self.Receive()
        match msg, state with
        | GetState ch, state ->
            state |> Option.map snd |> ch.Reply
            return! behaviour state self

        | Start ch, Some _ -> 
            ch.Reply (Some (errorf "A process is already running"))
            return! behaviour state self

        | Start ch, None ->
            match startProcess() with
            | Choice1Of2 p ->
                let _ = p.Exited.Subscribe(fun _ -> self.Post Recover)
                ch.Reply None
                return! behaviour (Some ([],p)) self

            | Choice2Of2 e ->
                ch.Reply (Some e)
                return! behaviour state self

        | Stop ch, None ->
            ch.Reply None
            return! behaviour state self
        
        | Stop ch, Some (_,proc) ->
            let error = try proc.Kill() ; None with e -> Some e
            ch.Reply error
            return! behaviour None self

        | Recover, None ->
            return! behaviour None self

        | Recover, Some (durations,proc) ->
            if (try proc.HasExited with _ -> true) then
                let pd = getDuration proc
                match retryPolicy.ShouldRetryAfter (pd :: durations) with
                | None ->
                    logger.Logf LogLevel.Warning "Giving up on restarts of child process."
                    return! behaviour None self

                | Some retryAfter ->
                    do! Async.Sleep (int retryAfter.TotalMilliseconds)
                    match startProcess() with
                    | Choice1Of2 p -> 
                        logger.Logf LogLevel.Info "Restarted child process (pid %d)" p.Id
                        let _ = p.Exited.Subscribe(fun _ -> self.Post Recover)
                        return! behaviour (Some (pd :: durations, p)) self
                    | Choice2Of2 e -> 
                        logger.Logf LogLevel.Error "Error starting child process: %O" e
                        return! behaviour None self
            else
                return! behaviour state self
    }

    let actor = MailboxProcessor.Start(behaviour None)

    member __.Start() =
        match actor.PostAndReply Start with
        | None -> ()
        | Some e -> raise e

    member __.Stop() =
        match actor.PostAndReply Stop with
        | None -> ()
        | Some e -> raise e

    member __.CurrentProcess =
        actor.PostAndReply GetState

    interface IDisposable with
        member __.Dispose() = __.Stop()

    static member Create(?executable:string, ?arguments:string, ?workingDirectory:string,
                            ?retryPolicy:ProcessMonitorRetryPolicy, ?logger:ISystemLogger) =

        let executable =
            match executable with
            | None -> System.Reflection.Assembly.GetEntryAssembly().Location
            | Some exe -> exe

        let arguments = defaultArg arguments ""
        let workingDirectory = defaultArg workingDirectory Environment.CurrentDirectory
        let retryPolicy =
            match retryPolicy with
            | Some rp -> rp
            | None -> 
                { new ProcessMonitorRetryPolicy with
                    member __.ShouldRetryAfter procDurations = 
                        if procDurations.Length > 10 &&  
                          (procDurations |> Seq.averageBy (fun d -> d.TotalSeconds)) < 10. then None
                        else Some(TimeSpan.FromSeconds 10.) }

        let logger = defaultArg logger (NullLogger() :> _)
        new ProcessMonitor(executable, arguments, workingDirectory, retryPolicy, logger)