namespace Nessos.MBrace.Runtime.Utils

open System.Threading.Tasks

[<AutoOpen>]
module Utils =
    
    let hset (xs : 'T seq) = new System.Collections.Generic.HashSet<'T>(xs)

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)