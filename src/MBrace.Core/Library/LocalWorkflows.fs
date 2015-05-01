/// Assortment of workflow combinators that act on collections within the local process.
module MBrace.Workflows.Local

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// Combinators that operate on inputs sequentially within the local process.
[<RequireQualifiedAccess>]
module Sequential =

    /// <summary>
    ///     Sequential map combinator.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Source sequence.</param>
    let map (mapper : 'T -> Local<'S>) (source : seq<'T>) : Local<'S []> = local {
        let results = new ResizeArray<'S> ()
        for t in source do
            let! s = mapper t
            results.Add s

        return results.ToArray()
    }

    /// <summary>
    ///     Sequential filter combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let filter (predicate : 'T -> Local<bool>) (source : seq<'T>) : Local<'T []> = local {
        let results = new ResizeArray<'T> ()
        for t in source do
            let! r = predicate t
            do if r then results.Add t

        return results.ToArray()
    }

    /// <summary>
    ///     Sequential choose combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let choose (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Local<'S []> = local {
        let results = new ResizeArray<'S> ()
        for t in source do
            let! r = chooser t
            do match r with Some s -> results.Add s | None -> ()
    
        return results.ToArray()
    }

    /// <summary>
    ///     Sequential fold combinator.
    /// </summary>
    /// <param name="folder">Folding function.</param>
    /// <param name="state">Initial state.</param>
    /// <param name="source">Input data.</param>
    let fold (folder : 'State -> 'T -> Local<'State>) (state : 'State) (source : seq<'T>) : Local<'State> = local {
        let state = ref state
        for t in source do
            let! state' = folder !state t
            state := state'

        return !state
    }

    /// <summary>
    ///     Sequential eager collect combinator.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Source data.</param>
    let collect (collector : 'T -> Local<#seq<'S>>) (source : seq<'T>) : Local<'S []> = local {
        let ra = new ResizeArray<'S> ()
        for t in source do
            let! ss = collector t
            do for s in ss do ra.Add(s)

        return ra.ToArray()
    }

    /// <summary>
    ///     Sequential lazy collect combinator.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Source data.</param>
    let lazyCollect (collector : 'T -> Local<#seq<'S>>) (source : seq<'T>) : Local<seq<'S>> = local {
        let! ctx = Cloud.GetExecutionContext()
        return seq {
            for t in source do yield! Cloud.RunSynchronously(collector t, ctx.Resources, ctx.CancellationToken)
        }
    }

    /// <summary>
    ///     Sequential tryFind combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let tryFind (predicate : 'T -> Local<bool>) (source : seq<'T>) : Local<'T option> = local {
        use e = source.GetEnumerator()
        let rec aux () = local {
            if e.MoveNext() then
                let! r = predicate e.Current
                if r then return Some e.Current
                else
                    return! aux ()
            else
                return None
        }

        return! aux ()
    }

    /// <summary>
    ///     Sequential tryPick combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let tryPick (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Local<'S option> = local {
        use e = source.GetEnumerator()
        let rec aux () = local {
            if e.MoveNext() then
                let! r = chooser e.Current
                match r with
                | None -> return! aux ()
                | Some _ -> return r
            else
                return None
        }

        return! aux ()
    }

    /// <summary>
    ///     Sequential iter combinator.
    /// </summary>
    /// <param name="body">Iterator body.</param>
    /// <param name="source">Input sequence.</param>
    let iter (body : 'T -> Local<unit>) (source : seq<'T>) : Local<unit> = local {
        for t in source do do! body t
    }


/// Collection of combinators that operate on inputs in parallel within the local process.
[<RequireQualifiedAccess>]
module Parallel =

    /// <summary>
    ///     Parallel map combinator.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Source sequence.</param>
    let map (mapper : 'T -> Local<'S>) (source : seq<'T>) : Local<'S []> = local {
        return! source |> Seq.map (fun t -> local { return! mapper t }) |> Local.Parallel
    }

    /// <summary>
    ///     Parallel filter combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let filter (predicate : 'T -> Local<bool>) (source : seq<'T>) : Local<'T []> = local {
        let! results = 
            source
            |> Seq.map (fun t -> local { let! result = predicate t in return if result then Some t else None })
            |> Local.Parallel

        return results |> Array.choose id
    }

    /// <summary>
    ///     Parallel choose combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let choose (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Local<'S []> = local {
        let! results = map chooser source
        return Array.choose id results
    }

    /// <summary>
    ///     Sequential eager collect combinator.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Source data.</param>
    let collect (collector : 'T -> Local<#seq<'S>>) (source : seq<'T>) : Local<'S []> = local {
        let! results = map (fun t -> local { let! rs = collector t in return Seq.toArray rs }) source
        return Array.concat results
    }

    /// <summary>
    ///     Sequential tryFind combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let tryFind (predicate : 'T -> Local<bool>) (source : seq<'T>) : Local<'T option> = local {
        let finder (t : 'T) = local { let! r = predicate t in return if r then Some t else None }
        return! source |> Seq.map finder |> Local.Choice
    }

    /// <summary>
    ///     Sequential tryPick combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let tryPick (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Local<'S option> = local {
        return! source |> Seq.map chooser |> Local.Choice
    }

    /// <summary>
    ///     Sequential iter combinator.
    /// </summary>
    /// <param name="body">Iterator body.</param>
    /// <param name="source">Input sequence.</param>
    let iter (body : 'T -> Local<unit>) (source : seq<'T>) : Local<unit> = local {
        let! _ = source |> Seq.map (fun t -> local { return! body t }) |> Local.Parallel
        return ()
    }