namespace MBrace.Runtime

open System
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Collections.Generic
open System.Collections.Concurrent

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

/// Represents a cloud computation that is being executed in the cluster.
[<AbstractClass>]
type CloudTask internal () =

    /// Gets the parent cancellation token for the cloud task
    abstract CancellationToken : ICloudCancellationToken

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
    abstract StartTime : DateTime option

    /// TimeSpan of executing process.
    abstract ExecutionTime : TimeSpan option

    /// DateTime of task completion
    abstract CompletionTime : DateTime option

    /// Active number of jobs related to the process.
    abstract ActiveJobs : int
    /// Max number of concurrently executing jobs for process.
    abstract MaxActiveJobs : int
    /// Number of jobs that have been completed for process.
    abstract CompletedJobs : int
    /// Number of faults encountered while executing jobs for process.
    abstract FaultedJobs : int
    /// Total number of jobs related to the process.
    abstract TotalJobs : int
    /// Process execution status.
    abstract Status : CloudTaskStatus

    /// Task identifier
    abstract Id : string
    /// Task user-supplied name
    abstract Name : string option
    /// Process return type
    abstract Type : Type

    /// Cancels execution of given process
    abstract Cancel : unit -> unit

    interface ICloudTask with
        member x.Id: string = x.Id

        member x.AwaitResultBoxed(?timeoutMilliseconds: int): Async<obj> = 
            x.AwaitResultBoxed(?timeoutMilliseconds = timeoutMilliseconds)
    
        member x.CancellationToken = x.CancellationToken
        member x.IsCanceled: bool = 
            match x.Status with
            | CloudTaskStatus.Canceled -> true
            | _ -> false
        
        member x.IsCompleted: bool = 
            match x.Status with
            | CloudTaskStatus.Completed -> true
            | _ -> false
        
        member x.IsFaulted: bool = 
            match x.Status with
            | CloudTaskStatus.Faulted | CloudTaskStatus.UserException -> true
            | _ -> false

        member x.ResultBoxed: obj = x.ResultBoxed
        member x.Status: TaskStatus = x.Status.TaskStatus
        member x.TryGetResultBoxed(): Async<obj option> = x.TryGetResultBoxed()

    /// Gets a printed report on the current process status
    member p.GetInfo() : string = CloudTaskReporter.Report([|p|], "Process", false)

    /// Prints a report on the current process status to stdout
    member p.ShowInfo () : unit = Console.WriteLine(p.GetInfo())

/// Represents a cloud computation that is being executed in the cluster.
and [<Sealed; DataContract; NoEquality; NoComparison>] CloudTask<'T> internal (source : ICloudTaskCompletionSource) =
    inherit CloudTask()

    [<DataMember(Name = "TaskSource")>]
    let entry = source

    [<IgnoreDataMember>]
    let mutable cell = Unchecked.defaultof<CacheAtom<CloudTaskState>>

    let init () =
        cell <- CacheAtom.Create(async { return! entry.GetState() }, intervalMilliseconds = 500)

    do init ()

    /// Triggers elevation in event of serialization
    [<OnSerializing>]
    member private c.OnDeserializing (_ : StreamingContext) = init ()

    /// <summary>
    ///     Asynchronously awaits task result
    /// </summary>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds. Defaults to infinite timeout.</param>
    member __.AwaitResult (?timeoutMilliseconds:int) : Async<'T> = async {
        let timeoutMilliseconds = defaultArg timeoutMilliseconds Timeout.Infinite
        let! result = Async.WithTimeout(async { return! entry.AwaitResult() }, timeoutMilliseconds) 
        return unbox<'T> result.Value
    }

    /// <summary>
    ///     Attempts to get task result. Returns None if not completed.
    /// </summary>
    member __.TryGetResult () : Async<'T option> = async {
        let! result = entry.TryGetResult()
        return result |> Option.map (fun r -> unbox<'T> r.Value)
    }

    /// Synchronously awaits task result 
    member __.Result : 'T = __.AwaitResult() |> Async.RunSync

    override __.AwaitResultBoxed (?timeoutMilliseconds:int) = async {
        let! r = __.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
        return box r
    }

    override __.TryGetResultBoxed () = async {
        let! r = __.TryGetResult()
        return r |> Option.map box
    }

    override __.ResultBoxed = __.Result |> box

    override __.StartTime =
        match cell.Value.ExecutionTime with
        | NotStarted -> None
        | Started(st,_) -> Some st
        | Finished(st,_,_) -> Some st

    override __.ExecutionTime =
        match cell.Value.ExecutionTime with
        | NotStarted -> None
        | Started(_,et) -> Some et
        | Finished(_,et,_) -> Some et

    override __.CompletionTime =
        match cell.Value.ExecutionTime with
        | Finished(_,_,ct) -> Some ct
        | _ -> None

    override __.CancellationToken = entry.Info.CancellationTokenSource.Token
    /// Active number of jobs related to the process.
    override __.ActiveJobs = cell.Value.ActiveJobCount
    override __.MaxActiveJobs = cell.Value.MaxActiveJobCount
    override __.CompletedJobs = cell.Value.CompletedJobCount
    override __.FaultedJobs = cell.Value.FaultedJobCount
    override __.TotalJobs = cell.Value.TotalJobCount
    override __.Status = cell.Value.Status
    override __.Id = entry.Id
    override __.Name = entry.Info.Name
    override __.Type = typeof<'T>
    override __.Cancel() = entry.Info.CancellationTokenSource.Cancel()


    interface ICloudTask<'T> with
        member x.AwaitResult(timeoutMilliseconds: int option): Async<'T> =
            x.AwaitResult(?timeoutMilliseconds = timeoutMilliseconds)
        
        member x.CancellationToken: ICloudCancellationToken = 
            entry.Info.CancellationTokenSource.Token
        
        member x.Result: 'T = x.Result
        
        member x.Status: TaskStatus = cell.Value.Status.TaskStatus
        
        member x.TryGetResult(): Async<'T option> = x.TryGetResult()

/// Cloud Process client object
and [<AutoSerializable(false)>] internal CloudTaskManagerClient(runtime : IRuntimeManager) =
    // TODO : add cleanup logic
    let tasks = new ConcurrentDictionary<string, CloudTask> ()

    /// <summary>
    ///     Fetches task by provided task id.
    /// </summary>
    /// <param name="taskId">Task identifier.</param>
    let getTaskBySource (entry : ICloudTaskCompletionSource) = async {
        let ok,t = tasks.TryGetValue entry.Id
        if ok then return t
        else
            let! assemblies = runtime.AssemblyManager.DownloadAssemblies(entry.Info.Dependencies)
            let loadInfo = runtime.AssemblyManager.LoadAssemblies(assemblies)
            for li in loadInfo do
                match li with
                | NotLoaded id -> runtime.SystemLogger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
                | LoadFault(id, e) -> runtime.SystemLogger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
                | Loaded _ -> ()

            let returnType = runtime.Serializer.UnPickleTyped entry.Info.ReturnType
            let ex = Existential.FromType returnType
            let task = ex.Apply { 
                new IFunc<CloudTask> with 
                    member __.Invoke<'T> () = new CloudTask<'T>(entry) :> CloudTask
            }

            return tasks.GetOrAdd(entry.Id, task)
    }

    member __.TryGetTaskById(id : string) = async {
        let! source = runtime.TaskManager.TryGetTaskById id
        match source with
        | None -> return None
        | Some e ->
            let! t = getTaskBySource e
            return Some t
    }


    member __.GetAllTasks() = async {
        let! entries = runtime.TaskManager.GetAllTasks()
        return!
            entries
            |> Seq.map getTaskBySource
            |> Async.Parallel
    }

    member __.ClearTask(task:CloudTask) = async {
        do! runtime.TaskManager.Clear(task.Id)
        ignore <| tasks.TryRemove(task.Id)
    }

    /// <summary>
    ///     Clears all processes from the runtime.
    /// </summary>
    member pm.ClearAllTasks() = async {
        do! runtime.TaskManager.ClearAllTasks()
        tasks.Clear()
    }

    /// Gets a printed report of all currently executing processes
    member pm.GetTaskInfo() : string =
        let procs = pm.GetAllTasks() |> Async.RunSync
        CloudTaskReporter.Report(procs, "Processes", borders = false)

    /// Prints a report of all currently executing processes to stdout.
    member pm.ShowTaskInfo() : unit =
        /// TODO : add support for filtering processes
        Console.WriteLine(pm.GetTaskInfo())

    /// Asynchronously fetches all task entries for supplied task object.
    member pm.GetTaskLogsAsync(task : CloudTask) : Async<seq<CloudLogEntry>> = async { 
        return! runtime.CloudLogManager.GetAllCloudLogEntriesByTask task.Id 
    }

    /// Prints all task entries for given task to stdout.
    member pm.ShowTaskLogs(task : CloudTask) : unit =
        let entries = runtime.CloudLogManager.GetAllCloudLogEntriesByTask task.Id |> Async.RunSync
        for e in entries do Console.WriteLine (e.ToString(showDate = true))
         
and internal CloudTaskReporter() = 
    static let template : Field<CloudTask> list = 
        [ Field.create "Name" Left (fun p -> match p.Name with Some n -> n | None -> "")
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "Status" Right (fun p -> sprintf "%A" p.Status)
          Field.create "Execution Time" Left (fun p -> Option.toNullable p.ExecutionTime)
          Field.create "Jobs" Center (fun p -> sprintf "%3d / %3d / %3d / %3d"  p.ActiveJobs p.FaultedJobs p.CompletedJobs p.TotalJobs)
          Field.create "Result Type" Left (fun p -> Type.prettyPrint p.Type) 
          Field.create "Start Time" Left (fun p -> Option.toNullable p.StartTime)
          Field.create "Completion Time" Left (fun p -> Option.toNullable p.CompletionTime)
        ]
    
    static member Report(processes : seq<CloudTask>, title : string, borders : bool) = 
        let ps = processes 
                 |> Seq.sortBy (fun p -> p.StartTime)
                 |> Seq.toList

        sprintf "%s\nJobs : Active / Faulted / Completed / Total\n" <| Record.PrettyPrint(template, ps, title, borders)