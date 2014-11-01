module internal Nessos.MBrace.SampleRuntime.RuntimeTypes

open System
open System.Threading.Tasks

open Nessos.Vagrant

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Serialization
open Nessos.MBrace.SampleRuntime.Actors

[<AutoSerializable(false)>]
type TaskExecutionMonitor () =
    let tcs = TaskCompletionSource<unit> ()
    static let fromContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskExecutionMonitor> ()

    member __.Task = tcs.Task
    member __.TriggerFault (e : exn) = tcs.TrySetException e |> ignore
    member __.TriggerCompletion () = tcs.TrySetResult () |> ignore

    static member ProtectSync ctx (f : unit -> unit) : unit =
        let tem = fromContext ctx
        try f () with e -> tem.TriggerFault e |> ignore

    static member ProtectAsync ctx (f : Async<unit>) : unit =
        let tem = fromContext ctx
        Async.StartWithContinuations(f, ignore, tem.TriggerFault, ignore)   

    static member TriggerCompletion ctx =
        let tem = fromContext ctx in tem.TriggerCompletion () |> ignore

    static member TriggerFault (ctx, e) =
        let tem = fromContext ctx in tem.TriggerFault e |> ignore

    static member AwaitCompletion (tem : TaskExecutionMonitor) = async {
        try
            return! Async.AwaitTask tem.Task
        with :? System.AggregateException as e when e.InnerException <> null ->
            return! Async.Reraise e.InnerException
    }

type Task = 
    {
        Type : Type
        Id : string
        StartTask : ExecutionContext -> unit
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    static member RunAsync (runtimeProvider : IRuntimeProvider) (deps : AssemblyId list) (task : Task) = async {
        let tem = new TaskExecutionMonitor()
        let ctx =
            {
                Resources = resource { yield runtimeProvider ; yield tem ; yield task.CancellationTokenSource ; yield (deps : AssemblyId list) }
                CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
            }

        do task.StartTask ctx
        return! TaskExecutionMonitor.AwaitCompletion tem
    }

type RuntimeState =
    {
        IPEndPoint : System.Net.IPEndPoint
        TaskQueue : Queue<Pickle<Task> * AssemblyId list>
        AssemblyExporter : AssemblyExporter
        CancellationTokenManager : CancellationTokenManager
        ResourceFactory : ResourceFactory
    }
with
    static member InitLocal () =
        {
            IPEndPoint = Nessos.MBrace.SampleRuntime.Config.getLocalEndpoint()
            TaskQueue = Queue<Pickle<Task> * AssemblyId list>.Init ()
            AssemblyExporter = AssemblyExporter.Init()
            CancellationTokenManager = CancellationTokenManager.Init()
            ResourceFactory = ResourceFactory.Init ()
        }

    member rt.EnqueueTask deps cts sc ec cc (wf : Cloud<'T>) =
        let taskId = System.Guid.NewGuid().ToString()
        let startTask ctx =
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx)
        
        let task = { Type = typeof<'T> ; Id = taskId ; StartTask = startTask ; CancellationTokenSource = cts ; }
        let taskp = Pickle.pickle task
        rt.TaskQueue.Enqueue(taskp, deps)

    member rt.StartAsCell deps cts (wf : Cloud<'T>) = async {
        let! resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            async {
                let! success = resultCell.SetResult r
                if success then TaskExecutionMonitor.TriggerCompletion ctx
                else
                    return failwith "FAULT : Could not commit to result cell."
            } |> TaskExecutionMonitor.ProtectAsync ctx

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueTask deps cts scont econt ccont wf
        return resultCell
    }

    member rt.TryDequeue () = async {
        let! item = rt.TaskQueue.TryDequeue()
        match item with
        | None -> return None
        | Some ((tp, deps), leaseMonitor) -> 
            do! rt.AssemblyExporter.LoadDependencies deps
            let task = Pickle.unpickle tp
            return Some (task, deps, leaseMonitor)
    }