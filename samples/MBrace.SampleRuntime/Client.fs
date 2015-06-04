namespace MBrace.SampleRuntime

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
open MBrace.Runtime.Store

/// MBrace Sample runtime client instance.
type MBraceRuntime private (state : RuntimeState, logger : AttacheableLogger) =
    inherit MBraceClient()
    static do Config.Init()
    static let mutable exe = None
    static let initWorkers (target : RuntimeState) (count : int) =
        if count < 1 then invalidArg "workerCount" "must be positive."
        let exe = MBraceRuntime.WorkerExecutable    
        let args = target.ToBase64()
        let psi = new ProcessStartInfo(exe, args)
        psi.WorkingDirectory <- Path.GetDirectoryName exe
        psi.UseShellExecute <- true
        for i = 1 to count do
            ignore <| Process.Start psi

    let manager = new ResourceManager(state, logger) :> IRuntimeResourceManager

    override __.Resources = manager

    member __.AttachLogger(l : ISystemLogger) = logger.AttachLogger l

    /// Violently kills all worker nodes in the runtime
    member __.KillAllWorkers () =
        let workers = manager.GetAvailableWorkers() |> Async.RunSync
        for w in workers do
            match w with
            | :? WorkerRef as w -> 
                let p = Process.GetProcessById w.Pid
                p.Kill()
            | _ -> ()

    /// Appens count of new worker processes to the runtime.
    member __.AppendWorkers (count : int) =
        let _ = initWorkers state count
        ()

    /// <summary>
    ///     Initialize a new local rutime instance with supplied worker count.
    /// </summary>
    /// <param name="workerCount"></param>
    /// <param name="fileStore"></param>
    static member InitLocal(workerCount : int, ?storeConfig : CloudFileStoreConfiguration, ?resource : ResourceRegistry) =
        if workerCount < 1 then invalidArg "workerCount" "must be positive."
        let storeConfig = 
            match storeConfig with 
            | Some c -> c
            | None -> 
                let fs = FileSystemStore.CreateSharedLocal()
                CloudFileStoreConfiguration.Create fs

        let logger = new AttacheableLogger()
        let state = RuntimeState.InitLocal(logger, storeConfig, ?miscResources = resource)
        let _ = initWorkers state workerCount
        new MBraceRuntime(state, logger)

    /// Gets or sets the worker executable location.
    static member WorkerExecutable
        with get () = match exe with None -> invalidOp "unset executable path." | Some e -> e
        and set path = 
            let path = Path.GetFullPath path
            if File.Exists path then exe <- Some path
            else raise <| FileNotFoundException(path)