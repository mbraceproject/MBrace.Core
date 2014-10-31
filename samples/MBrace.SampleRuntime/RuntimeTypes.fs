module internal Nessos.MBrace.SampleRuntime.RuntimeTypes

open System

open Nessos.Vagrant

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Serialization
open Nessos.MBrace.SampleRuntime.Actors

[<AutoSerializable(false)>]
type TaskCompletionEvent () = 
    inherit Event<exn option> ()
    static member inline OfContext (ctx : ExecutionContext) = ctx.Resources.Resolve<TaskCompletionEvent>()
    static member inline TriggerCompletion ctx = TaskCompletionEvent.OfContext(ctx).Trigger None
    static member inline TriggerFault ctx e = TaskCompletionEvent.OfContext(ctx).Trigger(Some e)

type Task = 
    {
        Type : Type
        Id : string
        StartTask : ExecutionContext -> unit
        FaultContinuation : ExecutionContext -> exn -> unit
        CancellationTokenSource : DistributedCancellationTokenSource
    }
with
    static member RunAsync (runtimeProvider : IRuntimeProvider) (deps : AssemblyId list) (task : Task) = async {
        let tce = new TaskCompletionEvent()
        let! awaitHandle = Async.StartChild(Async.AwaitEvent tce.Publish)
        let ctx =
            {
                Resources = resource { yield runtimeProvider ; yield tce ; yield task.CancellationTokenSource ; yield (deps : AssemblyId list) }
                CancellationToken = task.CancellationTokenSource.GetLocalCancellationToken()
            }

        do task.StartTask ctx
        let! result = awaitHandle
        match result with
        | None -> ()
        | Some e -> return raise e
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
            IPEndPoint = Actor.LocalEndPoint
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

        let faultCont = ec // TODO : wrap in FaultException
        
        let task = { Type = typeof<'T> ; Id = taskId ; StartTask = startTask ; FaultContinuation = faultCont ; CancellationTokenSource = cts ; }
        let taskp = Pickle.pickle task
        rt.TaskQueue.Enqueue(taskp, deps)

    member rt.StartAsCell deps cts (wf : Cloud<'T>) =
        let resultCell = rt.ResourceFactory.RequestResultCell<'T>()
        let setResult ctx r = 
            match (try resultCell.SetResult r |> Choice1Of2 with e -> Choice2Of2 e) with
            | Choice1Of2 true -> TaskCompletionEvent.TriggerCompletion ctx
            | Choice1Of2 false -> TaskCompletionEvent.TriggerFault ctx (new Exception("Could not set result."))
            | Choice2Of2 e -> TaskCompletionEvent.TriggerFault ctx e

        let scont ctx t = setResult ctx (Completed t)
        let econt ctx e = setResult ctx (Exception e)
        let ccont ctx c = setResult ctx (Cancelled c)
        rt.EnqueueTask deps cts scont econt ccont wf
        resultCell

    member rt.TryDequeue () = async {
        match rt.TaskQueue.TryDequeue() with
        | None -> return None
        | Some (tp, deps) -> 
            do! rt.AssemblyExporter.LoadDependencies deps
            let task = Pickle.unpickle tp
            return Some (task, deps)
    }