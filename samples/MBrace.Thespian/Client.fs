namespace MBrace.Thespian

open System
open System.IO
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading

open MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Thespian.Runtime

type ConsoleLogger = MBrace.Runtime.ConsoleLogger

/// MBrace.Thespian client object used to manage cluster and submit jobs for computation.
[<AutoSerializable(false)>]
type MBraceThespian private (manager : IRuntimeManager, state : RuntimeState, _logger : AttacheableLogger) =
    inherit MBraceClient(manager)
    static let processName = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name
    static do Config.Init()
    static let mutable exe = None
    static let initWorkers (target : RuntimeState) (count : int) =
        if count < 1 then invalidArg "workerCount" "must be positive."
        let exe = MBraceThespian.WorkerExecutable    
        let args = target.ToBase64()
        let psi = new ProcessStartInfo(exe, args)
        psi.WorkingDirectory <- Path.GetDirectoryName exe
        psi.UseShellExecute <- true
        for i = 1 to count do
            ignore <| Process.Start psi

    /// <summary>
    ///     Attaches user-supplied logger to client instance.
    /// </summary>
    /// <param name="logger">Logger instance to be attached.</param>
    member __.AttachLogger(logger : ISystemLogger) : IDisposable = _logger.AttachLogger logger

    /// Violently kills all worker nodes in the runtime
    member __.KillAllWorkers () =
        for w in base.Workers do
            try 
                if w.Hostname = MachineId.LocalInstance.Hostname then
                    let p = Process.GetProcessById w.ProcessId
                    if p.ProcessName = processName then
                        p.Kill()
            with _ -> ()

    /// <summary>
    ///     Spawns provided count of new local worker processes and attaches to the runtime.
    /// </summary>
    /// <param name="count">Number of workers to be spawned.</param>
    member __.AppendWorkers (count : int) =
        let _ = initWorkers state count
        ()

    /// <summary>
    ///     Initialize a new local rutime instance with supplied worker count.
    /// </summary>
    /// <param name="workerCount">Number of workers to spawn for cluster.</param>
    /// <param name="fileStore">File store configuration to be used for cluster.</param>
    /// <param name="resources">Resource registry to be appended to MBrace code.</param>
    static member InitLocal(workerCount : int, ?storeConfig : CloudFileStoreConfiguration, ?resources : ResourceRegistry) =
        if workerCount < 1 then invalidArg "workerCount" "must be positive."
        let storeConfig = 
            match storeConfig with 
            | Some c -> c
            | None -> 
                let fs = FileSystemStore.CreateSharedLocal()
                CloudFileStoreConfiguration.Create fs

        let logger = new AttacheableLogger()
        let state = RuntimeState.InitLocal(logger, storeConfig, ?miscResources = resources)
        let manager = new RuntimeManager(state, logger)
        let _ = initWorkers state workerCount
        new MBraceThespian(manager, state, logger)

    /// Gets or sets the worker executable location.
    static member WorkerExecutable
        with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            let path = Path.GetFullPath path
            if File.Exists path then exe <- Some path
            else raise <| FileNotFoundException(path)