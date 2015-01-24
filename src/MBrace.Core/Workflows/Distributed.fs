namespace MBrace.Workflows

open MBrace

/// Collection of distributed workflow combinators
module Distributed =

    /// <summary>
    ///     Distributed mapCombine combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="mapper">Map function that performs a local computation on a set of inputs.</param>
    /// <param name="combiner">Combiner function that composes two results.</param>
    /// <param name="init">Initial state and identity element of result space.</param>
    /// <param name="source">Input data.</param>
    let mapCombine (mapper : 'T [] -> Cloud<'State>) 
                    (combiner : 'State [] -> Cloud<'State>)
                    (init : 'State) (source : seq<'T>) : Cloud<'State> =

        let rec aux (inputs : 'T []) = cloud {
            if inputs.Length = 0 then return init else
            let! ctx = Cloud.GetSchedulingContext()
            match ctx with
            | Sequential -> return! mapper inputs
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
        mapCombine (Sequential.map mapper) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed filter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T []> =
        mapCombine (Sequential.filter predicate) (Cloud.lift Array.concat) [||] source

    /// <summary>
    ///     Distributed choose combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S []> =
        mapCombine (Sequential.choose chooser) (Cloud.lift Array.concat) [||] source

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

        mapCombine (Sequential.fold folder init) (Sequential.fold reducer init) init source

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

    //
    //  NonDeterministic Parallelism workflows
    //

    /// <summary>
    ///     Distributed tryPick combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick (chooser : 'T -> Cloud<'S option>) (source : seq<'T>) : Cloud<'S option> =
        let rec aux (inputs : 'T []) = cloud {
            if inputs.Length = 0 then return None else
            let! ctx = Cloud.GetSchedulingContext()
            match ctx with
            | Sequential -> return! Sequential.tryPick chooser inputs
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
    ///     Distributed tryFind combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind (predicate : 'T -> Cloud<bool>) (source : seq<'T>) : Cloud<'T option> =
        tryPick (fun t -> cloud { let! b = predicate t in return if b then Some t else None }) source