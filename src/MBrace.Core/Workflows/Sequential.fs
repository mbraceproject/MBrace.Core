namespace MBrace.Workflows

open MBrace

#nowarn "444"

/// Collection of cloud combinators with sequential execution semantics.
[<RequireQualifiedAccess>]
module Sequential =

    /// <summary>
    ///     Sequential map combinator.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Source sequence.</param>
    let map (mapper : 'T -> Cloud<'S>) (source : seq<'T>) : Cloud<'S []> = cloud {
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
    let filter (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T []> = cloud {
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
    let choose (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S []> = cloud {
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
    let fold (folder : 'State -> 'T -> Cloud<'State>) (state : 'State) (source : seq<'T>) : Cloud<'State> = cloud {
        let state = ref state
        for t in source do
            let! state' = folder !state t
            state := state'

        return !state
    }

    /// <summary>
    ///     Sequential tryFind combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let tryFind (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T option> = cloud {
        use e = source.GetEnumerator()
        let rec aux () = cloud {
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
    let tryPick (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S option> = cloud {
        use e = source.GetEnumerator()
        let rec aux () = cloud {
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