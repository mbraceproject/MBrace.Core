namespace MBrace.Runtime.Components

open System
open System.Diagnostics
open MBrace.Runtime
open Microsoft.FSharp.Control

[<NoEquality; NoComparison>]
type private ProcessMonitorMsg =
    | Stop of AsyncReplyChannel<exn option>
    | Start of AsyncReplyChannel<exn option>
    | GetState of AsyncReplyChannel<(TimeSpan list * Process) option>
    | Recover

type ProcessMonitorRetryPolicy =
    abstract ShouldRetryAfter : lastProcDurations:TimeSpan list -> TimeSpan option

/// Provides functionality for spawning and maintaining child processes.
/// Will automatically restart a child process if it dies.
[<AutoSerializable(false)>]
type ProcessMonitor private (executable : string, arguments : string, workingDirectory : string, 
                                redirectOutput : bool, retryPolicy : ProcessMonitorRetryPolicy, logger : ISystemLogger) =

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
            if redirectOutput then
                psi.RedirectStandardInput <- true
                psi.RedirectStandardError <- true
                proc.OutputDataReceived.Subscribe(fun args -> Console.WriteLine args.Data) |> ignore
                proc.ErrorDataReceived.Subscribe(fun args -> Console.Error.WriteLine args.Data) |> ignore

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
            ch.Reply state
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

    /// Starts the child process
    member __.Start() =
        match actor.PostAndReply Start with
        | None -> ()
        | Some e -> raise e

    /// Stops the child process
    member __.Stop() =
        match actor.PostAndReply Stop with
        | None -> ()
        | Some e -> raise e

    /// Gets the current child process, if available
    member __.CurrentProcess =
        actor.PostAndReply GetState |> Option.map snd

    /// Number of faults and restarts associated with current process instance
    member __.NumberOfRestarts =
        actor.PostAndReply GetState 
        |> Option.map (fun (ts,_) -> ts.Length)

    interface IDisposable with
        member __.Dispose() = __.Stop()

    /// <summary>
    ///     Creates a process monitoring instance with supplied parameters.
    /// </summary>
    /// <param name="executable">Path to executable that will be used to spawn monitored computation. Defaults to current process path.</param>
    /// <param name="arguments">CLI params to use with child computation. Defaults to empty.</param>
    /// <param name="workingDirectory">Working directory to be used by child. Default to current working directory.</param>
    /// <param name="redirectStandardOutput">Redirect stdout and stderr. Defaults to true.</param>
    /// <param name="retryPolicy">Retry policy on child process death.</param>
    /// <param name="logger">Logger to be used by the monitor.</param>
    static member Create(?executable:string, ?arguments:string, ?workingDirectory:string,
                            ?redirectStandardOutput:bool, ?retryPolicy:ProcessMonitorRetryPolicy, ?logger:ISystemLogger) =

        let executable =
            match executable with
            | None -> System.Reflection.Assembly.GetEntryAssembly().Location
            | Some exe -> exe

        let arguments = defaultArg arguments ""
        let workingDirectory = defaultArg workingDirectory Environment.CurrentDirectory
        let redirectStandardOutput = defaultArg redirectStandardOutput true
        let retryPolicy =
            match retryPolicy with
            | Some rp -> rp
            | None -> 
                { new ProcessMonitorRetryPolicy with
                    member __.ShouldRetryAfter procDurations = 
                        if procDurations.Length > 10 &&  
                          (procDurations |> Seq.averageBy (fun d -> d.TotalSeconds)) < 10. then None
                        else Some(TimeSpan.FromSeconds 10.) }

        let logger = match logger with Some l -> l | None -> NullLogger() :> _
        new ProcessMonitor(executable, arguments, workingDirectory, redirectStandardOutput, retryPolicy, logger)
