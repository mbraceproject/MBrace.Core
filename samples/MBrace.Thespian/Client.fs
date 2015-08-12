namespace MBrace.Thespian

open System
open System.IO
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading

open MBrace.Runtime

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Thespian.Runtime

/// A system logger that writes entries to stdout
type ConsoleLogger = MBrace.Runtime.ConsoleLogger
/// Log level used by the MBrace runtime implementation
type LogLevel = MBrace.Runtime.LogLevel

/// MBrace.Thespian client object used to manage cluster and submit jobs for computation.
[<AutoSerializable(false)>]
type MBraceThespian private (manager : IRuntimeManager, state : RuntimeState) =
    inherit MBraceClient(manager)
    static let processName = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name
    static do Config.Initialize(populateDirs = true)
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

    /// Violently kills all worker nodes in the runtime
    member __.KillAllWorkers () =
        for w in base.Workers do
            match w.WorkerId with
            | :? WorkerId as wid ->
                match wid.ProcessId.TryGetLocalProcess() with
                | Some p -> try p.Kill() with _ -> ()
                | None -> ()
            | _ -> ()

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
    /// <param name="logLevel">Set the cluster-wide system log level. Defaults to LogLevel.Info.</param>
    static member InitLocal(workerCount : int, ?storeConfig : CloudFileStoreConfiguration, ?resources : ResourceRegistry, ?logLevel : LogLevel) =
        if workerCount < 1 then invalidArg "workerCount" "must be positive."
        let storeConfig = 
            match storeConfig with 
            | Some c -> c
            | None -> 
                let fs = FileSystemStore.CreateSharedLocal()
                CloudFileStoreConfiguration.Create fs

        let state = RuntimeState.Create(storeConfig, ?miscResources = resources, ?logLevel = logLevel)
        let _ = initWorkers state workerCount
        new MBraceThespian(state.GetLocalRuntimeManager(), state)

    /// Gets or sets the worker executable location.
    static member WorkerExecutable
        with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            let path = Path.GetFullPath path
            if File.Exists path then exe <- Some path
            else raise <| FileNotFoundException(path)