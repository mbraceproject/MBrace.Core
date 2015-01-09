namespace MBrace.Runtime.Utils

open System.Collections.Concurrent
open System.Threading.Tasks

[<AutoOpen>]
module Utils =
    
    let hset (xs : 'T seq) = new System.Collections.Generic.HashSet<'T>(xs)

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)

    type Async with
        static member Parallel(left : Async<'T>, right : Async<'S>) = async {
            let box wf = async { let! r = wf in return box r }
            let! results = Async.Parallel [| box left ; box right |]
            return results.[0] :?> 'T, results.[1] :?> 'S
        }


    type ConcurrentDictionary<'K,'V> with
        member dict.TryAdd(key : 'K, value : 'V, ?forceUpdate) =
            if defaultArg forceUpdate false then
                let _ = dict.AddOrUpdate(key, value, fun _ _ -> value)
                true
            else
                dict.TryAdd(key, value)

    type Event<'T> with
        member e.TriggerAsTask(t : 'T) =
            System.Threading.Tasks.Task.Factory.StartNew(fun () -> e.Trigger t)