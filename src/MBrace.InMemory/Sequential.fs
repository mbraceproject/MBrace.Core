namespace Nessos.MBrace.InMemory

open System.Threading

open Nessos.MBrace
open Nessos.MBrace.Runtime

/// Collection of context-less combinators for 
/// execution within local thread context.
type Sequential =

    /// <summary>
    ///     Provides a context-less Cloud.Parallel implementation
    ///     for execution within the current thread.
    /// </summary>
    /// <param name="computations">Input computations</param>
    static member Parallel (computations : seq<Cloud<'T>>) = cloud {
        let computations = Seq.toArray computations
        let results = Array.zeroCreate<'T> computations.Length
        let rec aux i = cloud {
            if i = computations.Length then return results
            else
                let! t = computations.[i]
                results.[i] <- t
                return! aux (i+1)
        }

        return! aux 0
    }

    /// <summary>
    ///     Provides a context-less Cloud.Choice implementation
    ///     for execution within the current thread.
    /// </summary>
    /// <param name="computations">Input computations</param>
    static member Choice (computations : seq<Cloud<'T option>>) = cloud {
        let computations = Seq.toArray computations

        let rec aux i = cloud {
            if i = computations.Length then return None
            else
                let! topt = computations.[i]
                if Option.isSome topt then return topt
                else
                    return! aux (i+1)
        }

        return! aux 0
    }

    /// <summary>
    ///     Provides a context-less Cloud.StartChild implementation
    ///     for execution within the current thread context.
    /// </summary>
    /// <param name="computation">Input computation</param>
    static member StartChild (computation : Cloud<'T>) = cloud {
        let! result = computation |> Cloud.Catch
        return cloud {  
            match result with 
            | Choice1Of2 t -> return t
            | Choice2Of2 e -> return! Cloud.Raise e
        }
    }