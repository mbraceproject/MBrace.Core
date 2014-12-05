namespace Nessos.MBrace

open Nessos.MBrace.InMemoryRuntime

/// Collection of mapReduce and related workflows
module MapReduce =

    /// <summary>
    ///     Parallel fold combinator
    /// </summary>
    /// <param name="folder">Folding workflow</param>
    /// <param name="id">Reduction identity element</param>
    /// <param name="reducer">Reducing function.</param>
    /// <param name="inputs">Inputs</param>
    let parFold (folder : 'State -> 'T -> Cloud<'State>)
                    (id : 'State) (reducer : 'State -> 'State -> Cloud<'State>)
                    (inputs : 'T []) : Cloud<'State> =
    
        let rec aux (inputs : 'T []) = cloud {
            if inputs.Length = 0 then return id else
            let! ctx = Cloud.GetSchedulingContext()
            match ctx with
            | Sequential -> return! Sequential.fold folder id inputs
            | ThreadParallel ->
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                let! results = 
                    chunks 
                    |> Array.map (Cloud.ToSequential << aux)
                    |> Cloud.Parallel

                return! Sequential.fold reducer id results

            | Distributed ->
                let! size = Cloud.GetWorkerCount()
                let chunks = Array.splitByPartitionCount size inputs
                let! results =
                    chunks
                    |> Array.map (Cloud.ToLocal << aux)
                    |> Cloud.Parallel

                return! Sequential.fold reducer id results
        }

        aux inputs

    /// <summary>
    ///     Map/Reduce implementation.
    /// </summary>
    /// <param name="mapper">Mapper function</param>
    /// <param name="id">Result identity element.</param>
    /// <param name="reducer">Reducer function.</param>
    /// <param name="inputs">Inputs.</param>
    let mapReduce (mapper : 'T -> Cloud<'R>)
                  (id : 'R) (reducer : 'R -> 'R -> Cloud<'R>)
                  (inputs : 'T []) =
    
        parFold (fun s t -> cloud { let! s' = mapper t in return! reducer s s'})
                id reducer inputs