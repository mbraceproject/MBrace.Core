namespace MBrace.Workflows

open MBrace

[<RequireQualifiedAccess>]
module Cloud =

    /// <summary>
    ///     Embeds a value in cloud workflow.
    /// </summary>
    /// <param name="t">Value to be embedded.</param>
    let ret (t : 'T) : Workflow<_, 'T> = wfb { return t }

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift (f : 'T -> 'S) (t : 'T) : Workflow<_, 'S> = wfb { return f t }

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift2 (f : 'T1 -> 'T2 -> 'S) (t1 : 'T1) (t2 : 'T2) : Workflow<_, 'S> = wfb { return f t1 t2 }

    /// <summary>
    ///     Cloud workflow map combinator.
    /// </summary>
    /// <param name="mapper">Mapping function.</param>
    /// <param name="tworkflow">Input workflow.</param>
    let map (mapper : 'T -> 'S) (tworkflow : Workflow<_, 'T>) = wfb { let! t = tworkflow in return mapper t }


/// collection of parallelism operators for the cloud
[<AutoOpen>]
module CloudOperators =
        
    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<||>) (left : Workflow<'a>) (right : Workflow<'b>) : Cloud<'a * 'b> = 
        cloud { 
            let left'= cloud { let! value = left in return value :> obj }
            let right' = cloud { let! value = right in return value :> obj }
            let! result = Cloud.Parallel [| left' ; right' |]
            return (result.[0] :?> 'a, result.[1] :?> 'b) 
        }

    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel and returns the
    ///     result of the first computation that completes and cancels the other.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<|>) (left : Workflow<'a>) (right : Workflow<'a>) : Cloud<'a> =
        cloud {
            let left' = cloud { let! value = left  in return Some (value) }
            let right'= cloud { let! value = right in return Some (value) }
            let! result = Cloud.Choice [| left' ; right' |]
            return result.Value
        }

    /// <summary>
    ///     Combines two cloud computations into one that executes them sequentially.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }

    /// <summary>
    ///     Send a message to cloud channel.
    /// </summary>
    /// <param name="channel">Target channel.</param>
    /// <param name="msg">Input message.</param>
    let inline (<--) (channel : ISendPort<'T>) (message : 'T) : Local<unit> = channel.Send message