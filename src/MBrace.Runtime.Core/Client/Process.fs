namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.Threading.Tasks

open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

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

    /// Date of process execution start.
    member __.StartTime =
        match cell.Value.ExecutionTime with
        | NotStarted -> None
        | Started(st,_) -> Some st
        | Finished(st,_,_) -> Some st

    /// TimeSpan of executing process.
    member __.ExecutionTime =
        match cell.Value.ExecutionTime with
        | NotStarted -> None
        | Started(_,et) -> Some et
        | Finished(_,et,_) -> Some et

    /// DateTime of task completion
    member __.CompletionTime =
        match cell.Value.ExecutionTime with
        | Finished(_,_,ct) -> Some ct
        | _ -> None

    /// Active number of jobs related to the process.
    member __.ActiveJobs = cell.Value.ActiveJobCount
    /// Max number of concurrently executing jobs for process.
    member __.MaxActiveJobs = cell.Value.MaxActiveJobCount
    /// Number of jobs that have been completed for process.
    member __.CompletedJobs = cell.Value.CompletedJobCount
    /// Number of faults encountered while executing jobs for process.
    member __.FaultedJobs = cell.Value.FaultedJobCount
    /// Total number of jobs related to the process.
    member __.TotalJobs = cell.Value.TotalJobCount
    /// Process execution status.
    member __.Status = cell.Value.Status

    /// Process identifier
    member __.Id = tcs.Info.Id
    /// Process name
    member __.Name = tcs.Info.Name
    /// Process return type
    member __.Type = tcs.Type
    /// Cancels execution of given process
    member __.Cancel() = tcs.CancellationTokenSource.Cancel()

    /// Gets a printed report on the current process status
    member p.GetInfo() : string = ProcessReporter.Report([|p|], "Process", false)

    /// Prints a report on the current process status to stdout
    member p.ShowInfo () : unit = Console.WriteLine(p.GetInfo())

/// Cloud process client object
and [<Sealed; AutoSerializable(false)>]
  CloudProcess<'T> internal (tcs : ICloudTaskCompletionSource<'T>, manager : ICloudTaskManager) =
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
and [<AutoSerializable(false)>] internal
  CloudProcessManager(runtime : IRuntimeManager) =
    // TODO : add cleanup logic
    let processes = new ConcurrentDictionary<string, CloudProcess> ()

    /// <summary>
    ///     Fetches task by provided task id.
    /// </summary>
    /// <param name="taskId">Task identifier.</param>
    member __.GetProcessById (taskId : string) = async {
        let ok,p = processes.TryGetValue taskId
        if ok then return p
        else
            let! info = runtime.TaskManager.GetTaskState(taskId)
            let! assemblies = runtime.AssemblyManager.DownloadAssemblies(info.Info.Dependencies)
            let loadInfo = runtime.AssemblyManager.LoadAssemblies(assemblies)
            for li in loadInfo do
                match li with
                | NotLoaded id -> runtime.SystemLogger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
                | LoadFault(id, e) -> runtime.SystemLogger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
                | Loaded _ -> ()

            let! tcs = runtime.TaskManager.GetTaskCompletionSourceById(taskId)
            let e = Existential.FromType tcs.Type
            let proc = e.Apply { 
                new IFunc<CloudProcess> with 
                    member __.Invoke<'T> () = 
                        let tcs = tcs :?> ICloudTaskCompletionSource<'T>
                        new CloudProcess<'T>(tcs, runtime.TaskManager) :> CloudProcess
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
            let p = processes.GetOrAdd(task.Info.Id, (fun _ -> new CloudProcess<'T>(task, runtime.TaskManager) :> CloudProcess))
            p :?> CloudProcess<'T>

    /// <summary>
    ///     Gets all processes running in the cluster.
    /// </summary>
    member pm.GetAllProcesses() = async {
        let! state = runtime.TaskManager.GetAllTasks()
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
        do! runtime.TaskManager.Clear(proc.Id)
        processes.TryRemove(proc.Id) |> ignore
    }

    /// <summary>
    ///     Clears all processes from the runtime.
    /// </summary>
    member pm.ClearAllProcesses() = async {
        do! runtime.TaskManager.ClearAllTasks()
        processes.Clear()
    }

    /// Gets a printed report of all currently executing processes
    member pm.GetProcessInfo() : string =
        let procs = pm.GetAllProcesses() |> Async.RunSync
        ProcessReporter.Report(procs, "Processes", borders = false)

    /// Prints a report of all currently executing processes to stdout.
    member pm.ShowProcessInfo() : unit =
        /// TODO : add support for filtering processes
        Console.WriteLine(pm.GetProcessInfo())

and internal ProcessReporter() = 
    static let template : Field<CloudProcess> list = 
        [ Field.create "Name" Left (fun p -> match p.Name with Some n -> n | None -> "")
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "Status" Right (fun p -> sprintf "%A" p.Status)
//          Field.create "Completed" Left (fun p -> p.Status = CloudTaskStatus.)
          Field.create "Execution Time" Left (fun p -> Option.toNullable p.ExecutionTime)
          Field.create "Jobs" Center (fun p -> sprintf "%3d / %3d / %3d / %3d"  p.ActiveJobs p.FaultedJobs p.CompletedJobs p.TotalJobs)
          Field.create "Result Type" Left (fun p -> Type.prettyPrint p.Type) 
          Field.create "Start Time" Left (fun p -> Option.toNullable p.StartTime)
          Field.create "Completion Time" Left (fun p -> Option.toNullable p.CompletionTime)
        ]
    
    static member Report(processes : seq<CloudProcess>, title : string, borders : bool) = 
        let ps = processes 
                 |> Seq.sortBy (fun p -> p.StartTime)
                 |> Seq.toList

        sprintf "%s\nJobs : Active / Faulted / Completed / Total\n" <| Record.PrettyPrint(template, ps, title, borders)