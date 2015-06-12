namespace MBrace.Runtime

open System
open System.Collections.Concurrent

open Nessos.Vagabond

open MBrace.Core
open MBrace.Runtime.Utils

/// Cloud process client object
[<AbstractClass; AutoSerializable(false)>]
type CloudProcess internal (tcs : ICloudTaskCompletionSource, manager : ICloudTaskManager) =
    let cell = CacheAtom.Create(async { return! manager.GetTaskState(tcs.Info.Id) }, intervalMilliseconds = 200)

    /// <summary>
    ///     Asynchronously awaits boxed result of given cloud process.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    abstract AwaitResultBoxed : ?timeoutMilliseconds:int -> Async<obj>
    /// <summary>
    ///     Return the result if available or None if not available.
    /// </summary>
    abstract TryGetResultBoxed : unit -> Async<obj option>
    /// Awaits the boxed result of the process.
    abstract ResultBoxed : obj
    /// Process execution status
    member __.Status = cell.Value.Status
    /// Process identifier
    member __.Id = tcs.Info.Id
    /// Process name
    member __.Name = tcs.Info.Name
    /// Process return type
    member __.Type = tcs.Type
    /// Cancels execution of given process
    member __.Cancel() = tcs.CancellationTokenSource.Cancel()

/// Cloud process client object
[<Sealed; AutoSerializable(false)>]
type CloudProcess<'T> internal (tcs : ICloudTaskCompletionSource<'T>, manager : ICloudTaskManager) =
    inherit CloudProcess(tcs, manager)

    /// Serializable CloudTask corresponding to the process object
    member __.Task : ICloudTask<'T> = tcs.Task

    /// <summary>
    ///     Asynchronously awaits result of given cloud process.
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member __.AwaitResult(?timeoutMilliseconds:int) : Async<'T> = tcs.Task.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)

    /// <summary>
    ///     Return the result if available or None if not available.
    /// </summary>
    member __.TryGetResult() : Async<'T option> = tcs.Task.TryGetResult()

    /// Awaits the result of the process.
    member __.Result : 'T = tcs.Task.Result

    override __.AwaitResultBoxed(?timeoutMilliseconds:int) = async {
        let! result = tcs.Task.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
        return box result
    }

    override __.TryGetResultBoxed() = async {
        let! result = tcs.Task.TryGetResult()
        return result |> Option.map box
    }

    override __.ResultBoxed = box tcs.Task.Result

/// Cloud Process client object
[<AutoSerializable(false)>]
type CloudProcessManager(resources : IRuntimeResourceManager) =
    // TODO : add cleanup logic
    // TODO : add pretty printing
    let processes = new ConcurrentDictionary<string, CloudProcess> ()

    /// <summary>
    ///     Fetches task by provided task id.
    /// </summary>
    /// <param name="taskId">Task identifier.</param>
    member __.GetProcessById (taskId : string) = async {
        let ok,p = processes.TryGetValue taskId
        if ok then return p
        else
            let! info = resources.TaskManager.GetTaskState(taskId)
            let! assemblies = resources.AssemblyManager.DownloadAssemblies(info.Info.Dependencies)
            let loadInfo = resources.AssemblyManager.LoadAssemblies(assemblies)
            for li in loadInfo do
                match li with
                | NotLoaded id -> resources.SystemLogger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
                | LoadFault(id, e) -> resources.SystemLogger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
                | Loaded _ -> ()

            let! tcs = resources.TaskManager.GetTaskCompletionSourceById(taskId)
            let e = Existential.FromType tcs.Type
            let proc = e.Apply { 
                new IFunc<CloudProcess> with 
                    member __.Invoke<'T> () = 
                        let tcs = tcs :?> ICloudTaskCompletionSource<'T>
                        new CloudProcess<'T>(tcs, resources.TaskManager) :> CloudProcess
            }

            return processes.GetOrAdd(taskId, proc)
    }

    /// <summary>
    ///     Gets a process object by task completion source.
    /// </summary>
    /// <param name="task">Input task completion source.</param>
    member pm.GetProcess(task : ICloudTaskCompletionSource<'T>) : CloudProcess<'T> =
        // TODO: add validation check for task
        let ok, p = processes.TryGetValue task.Info.Id
        if ok then p :?> CloudProcess<'T>
        else
            let p = processes.GetOrAdd(task.Info.Id, (fun _ -> new CloudProcess<'T>(task, resources.TaskManager) :> CloudProcess))
            p :?> CloudProcess<'T>

    /// <summary>
    ///     Gets all processes running in the cluster.
    /// </summary>
    member pm.GetAllProcesses() = async {
        let! state = resources.TaskManager.GetAllTasks()
        return! 
            state 
            |> Seq.map (fun s -> s.Info.Id) 
            |> Seq.distinct
            |> Seq.map pm.GetProcessById
            |> Async.Parallel
    }

    /// <summary>
    ///     Clears information for provided process from cluster.
    /// </summary>
    /// <param name="proc">Process to be cleared.</param>
    member pm.ClearProcess(proc : CloudProcess) = async {
        do! resources.TaskManager.Clear(proc.Id)
        processes.TryRemove(proc.Id) |> ignore
    }

    /// <summary>
    ///     Clears all processes from the runtime.
    /// </summary>
    member pm.ClearAllProcesses() = async {
        do! resources.TaskManager.ClearAllTasks()
        processes.Clear()
    }