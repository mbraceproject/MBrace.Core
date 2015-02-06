namespace MBrace.Workflows

open MBrace

/// Collection of distributed workflow combinators
module Distributed =

    /// <summary>
    ///     Distributed reduceCombine combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="reducer">Sequential reducer workflow.</param>
    /// <param name="combiner">Combiner function that sequentially composes a collection of results.</param>
    /// <param name="init">Initial state and identity element of result space.</param>
    /// <param name="source">Input data.</param>
    let reduceCombine (reducer : 'T [] -> Cloud<'State>) 
                        (combiner : 'State [] -> Cloud<'State>)
                        (init : 'State) (source : seq<'T>) : Cloud<'State> =

        let rec aux (inputs : 'T []) = cloud {
            match inputs.Length with
            | 0 -> return init
            | 1 -> return! reducer inputs
            | _ ->

            let! ctx = Cloud.GetSchedulingContext()
            match ctx with
            | Sequential -> return! reducer inputs
            | ThreadParallel ->
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                let! results = 
                    chunks 
                    |> Array.map (Cloud.ToSequential << aux)
                    |> Cloud.Parallel

                return! combiner results

            | Distributed ->
                let! size = Cloud.GetWorkerCount()
                let chunks = Array.splitByPartitionCount size inputs
                let! results =
                    chunks
                    |> Array.map (Cloud.ToLocal << aux)
                    |> Cloud.Parallel

                return! combiner results
        }
        
        cloud { return! aux (Seq.toArray source) }

    /// <summary>
    ///     Distributed map combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let map (mapper : 'T -> Cloud<'S>) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (Sequential.map mapper) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed map combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let map2 (mapper : 'T -> 'S) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (Cloud.lift <| Array.map mapper) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed filter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (Sequential.filter predicate) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed filter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter2 (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (Cloud.lift <| Array.filter predicate) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed choose combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (Sequential.choose chooser) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed choose combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose2 (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (Cloud.lift <| Array.choose chooser) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed fold combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="folder">Folding workflow.</param>
    /// <param name="reducer">Reducing function.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input data.</param>
    let fold (folder : 'State -> 'T -> Cloud<'State>)
                (reducer : 'State -> 'State -> Cloud<'State>)
                (init : 'State) (source : seq<'T>) : Cloud<'State> =

        reduceCombine (Sequential.fold folder init) (Sequential.fold reducer init) init source

    /// <summary>
    ///     Distributed fold combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="folder">Folding workflow.</param>
    /// <param name="reducer">Reducing function.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input data.</param>
    let fold2 (folder : 'State -> 'T -> 'State)
                (reducer : 'State -> 'State -> 'State)
                (init : 'State) (source : seq<'T>) : Cloud<'State> =

        reduceCombine (Cloud.lift <| Array.fold folder init) (Cloud.lift <| Array.reduce reducer) init source

    /// <summary>
    ///     Distrbuted collect combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collect (collector : 'T -> Cloud<#seq<'S>>) (source : seq<'T>) =
        reduceCombine (Sequential.collect collector) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distrbuted collect combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collect2 (collector : 'T -> #seq<'S>) (source : seq<'T>) =
        reduceCombine (Cloud.lift <| Array.collect (Seq.toArray << collector)) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed Map/Reduce workflow with cluster balancing.
    /// </summary>
    /// <param name="mapper">Mapper workflow.</param>
    /// <param name="reducer">Reducer workflow.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input source.</param>
    let mapReduce (mapper : 'T -> Cloud<'R>)
                  (reducer : 'R -> 'R -> Cloud<'R>)
                  (init : 'R) (source : seq<'T>) : Cloud<'R> =

        fold (fun s t -> cloud { let! s' = mapper t in return! reducer s s'})
                reducer init source

    /// <summary>
    ///     Distributed Map/Reduce workflow with cluster balancing.
    /// </summary>
    /// <param name="mapper">Mapper workflow.</param>
    /// <param name="reducer">Reducer workflow.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input source.</param>
    let mapReduce2 (mapper : 'T -> 'R)
                      (reducer : 'R -> 'R -> 'R)
                      (init : 'R) (source : seq<'T>) : Cloud<'R> =

        fold2 (fun s t -> let s' = mapper t in reducer s s')
                reducer init source

    //
    //  NonDeterministic Parallelism workflows
    //

    /// <summary>
    ///     Distributed search combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function acting on partition.</param>
    /// <param name="source">Input data.</param>
    let search (chooser : 'T [] -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S option> =
        let rec aux (inputs : 'T []) = cloud {
            match inputs.Length with
            | 0 -> return None
            | 1 -> return! chooser inputs
            | _ ->

            let! ctx = Cloud.GetSchedulingContext()
            match ctx with
            | Sequential -> return! chooser inputs
            | ThreadParallel ->
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                return!
                    chunks 
                    |> Array.map (Cloud.ToSequential << aux)
                    |> Cloud.Choice

            | Distributed ->
                let! size = Cloud.GetWorkerCount()
                let chunks = Array.splitByPartitionCount size inputs
                return!
                    chunks
                    |> Array.map (Cloud.ToLocal << aux)
                    |> Cloud.Choice
        }

        cloud { return! aux (Seq.toArray source) }

    /// <summary>
    ///     Distributed tryPick combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S option> =
        search (Sequential.tryPick chooser) source

    /// <summary>
    ///     Distributed tryPick combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick2 (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S option> =
        search (Cloud.lift <| Array.tryPick chooser) source

    /// <summary>
    ///     Distributed tryFind combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T option> =
        tryPick (fun t -> cloud { let! b = predicate t in return if b then Some t else None }) source

    /// <summary>
    ///     Distributed tryFind combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind2 (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T option> =
        tryPick2 (fun t -> if predicate t then Some t else None) source