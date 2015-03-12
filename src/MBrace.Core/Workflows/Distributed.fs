namespace MBrace.Workflows

open MBrace

/// Collection of combinators that split workloads to workers
/// according to multicore capacity.
module DivideAndConquer =

    /// <summary>
    ///     Distributed reduceCombine combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="reducer">Sequential reducer workflow.</param>
    /// <param name="combiner">Combiner function that sequentially composes a collection of results.</param>
    /// <param name="source">Input data.</param>
    let reduceCombine (reducer : 'T [] -> Local<'State>) 
                        (combiner : 'State [] -> Local<'State>) 
                            (source : seq<'T>) : Cloud<'State> =

        let reduceCombineLocal (inputs : 'T[]) = local {
            if inputs.Length < 2 then return! reducer inputs
            else
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                let! results =
                    chunks
                    |> Array.map reducer
                    |> Local.Parallel

                return! combiner results
        }

        cloud {
            let inputs = Seq.toArray source
            if inputs.Length < 2 then return! reducer inputs
            else
                let! workers = Cloud.GetAvailableWorkers()
                let weights = workers |> Array.map (fun w -> w.ProcessorCount)
                let chunks = Array.splitWeighted weights inputs
                let! results =
                    chunks
                    |> Array.map (fun (i,ts) -> reduceCombineLocal ts, workers.[i])
                    |> Cloud.Parallel

                return! combiner results
        }

    /// <summary>
    ///     Distributed map combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let map (mapper : 'T -> Local<'S>) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (Sequential.map mapper) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed map combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let map2 (mapper : 'T -> 'S) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (Local.lift <| Array.map mapper) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed filter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter (predicate : 'T -> Local<bool>) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (Sequential.filter predicate) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed filter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter2 (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (Local.lift <| Array.filter predicate) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed choose combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (Sequential.choose chooser) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed choose combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose2 (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (Local.lift <| Array.choose chooser) (Local.lift Array.concat) source

    /// <summary>
    ///     Distrbuted collect combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collect (collector : 'T -> Local<#seq<'S>>) (source : seq<'T>) =
        reduceCombine (Sequential.collect collector) (Local.lift Array.concat) source

    /// <summary>
    ///     Distrbuted collect combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collect2 (collector : 'T -> #seq<'S>) (source : seq<'T>) =
        reduceCombine (Local.lift <| Array.collect (Seq.toArray << collector)) (Local.lift Array.concat) source

    /// <summary>
    ///     Distributed fold combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="folder">Folding workflow.</param>
    /// <param name="reducer">Reducing function.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input data.</param>
    let fold (folder : 'State -> 'T -> Local<'State>)
                (reducer : 'State -> 'State -> Local<'State>)
                (init : 'State) (source : seq<'T>) : Cloud<'State> =

        reduceCombine (Sequential.fold folder init) (Sequential.fold reducer init) source

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

        let reduce inputs =
            if Array.isEmpty inputs then init
            else
                Array.reduce reducer inputs

        reduceCombine (Local.lift <| Array.fold folder init) (Local.lift reduce) source

    /// <summary>
    ///     Distributed iter combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="body">Iterator body.</param>
    /// <param name="source">Input sequence.</param>
    let iter (body : 'T -> Local<unit>) (source : seq<'T>) : Cloud<unit> =
        reduceCombine (Sequential.iter body) (fun _ -> local.Zero()) source

    /// <summary>
    ///     Distributed Map/Reduce workflow with cluster balancing.
    /// </summary>
    /// <param name="mapper">Mapper workflow.</param>
    /// <param name="reducer">Reducer workflow.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input source.</param>
    let mapReduce (mapper : 'T -> Local<'R>)
                  (reducer : 'R -> 'R -> Local<'R>)
                  (init : 'R) (source : seq<'T>) : Cloud<'R> =

        fold (fun s t -> local { let! s' = mapper t in return! reducer s s'})
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
    let search (chooser : 'T [] -> Local<'S option>) (source : seq<'T>) : Cloud<'S option> =
        let multiCoreSearch (inputs : 'T []) = local {
            if inputs.Length < 2 then return! chooser inputs 
            else
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                return!
                    chunks
                    |> Array.map chooser
                    |> Local.Choice
        }

        cloud {
            let inputs = Seq.toArray source
            if inputs.Length < 2 then return! chooser inputs
            else
                let! workers = Cloud.GetAvailableWorkers()
                let weights = workers |> Array.map (fun w -> w.ProcessorCount)
                let chunks = Array.splitWeighted weights inputs
                return!
                    chunks
                    |> Array.map (fun (i,ch) -> multiCoreSearch ch, workers.[i])
                    |> Cloud.Choice
        }

    /// <summary>
    ///     Distributed tryPick combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick (chooser : 'T -> Local<'S option>) (source : seq<'T>) : Cloud<'S option> =
        search (Sequential.tryPick chooser) source

    /// <summary>
    ///     Distributed tryPick combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick2 (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S option> =
        search (Local.lift <| Array.tryPick chooser) source

    /// <summary>
    ///     Distributed tryFind combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind (predicate : 'T -> Local<bool>) (source : seq<'T>) : Cloud<'T option> =
        tryPick (fun t -> local { let! b = predicate t in return if b then Some t else None }) source

    /// <summary>
    ///     Distributed tryFind combinator. Input data is partitioned according to cluster size
    ///     and distributed to worker nodes accordingly. It is then further partitioned
    ///     according to the processor count of each worker.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind2 (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T option> =
        tryPick2 (fun t -> if predicate t then Some t else None) source