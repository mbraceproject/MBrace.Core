namespace MBrace.Workflows

open MBrace

[<RequireQualifiedAccess>]
module Cloud =

    /// <summary>
    ///     Embeds a value in cloud workflow.
    /// </summary>
    /// <param name="t">Value to be embedded.</param>
    let inline ret (t : 'T) : Cloud<'T> = cloud.Return t

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let inline lift (f : 'T -> 'S) (t : 'T) : Cloud<'S> = cloud { return f t }

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let inline lift2 (f : 'T1 -> 'T2 -> 'S) (t1 : 'T1) (t2 : 'T2) : Cloud<'S> = cloud { return f t1 t2 }

    /// <summary>
    ///     Cloud workflow map combinator.
    /// </summary>
    /// <param name="mapper">Mapping function.</param>
    /// <param name="tworkflow">Input workflow.</param>
    let inline map (mapper : 'T -> 'S) (tworkflow : Cloud<'T>) = cloud { let! t = tworkflow in return mapper t }


/// collection of parallelism operators for the cloud
[<AutoOpen>]
module CloudOperators =
        
    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
        cloud { 
            let! result = 
                    Cloud.Parallel<obj> [| cloud { let! value = left in return value :> obj }; 
                                            cloud { let! value = right in return value :> obj } |]
            return (result.[0] :?> 'a, result.[1] :?> 'b) 
        }

    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel and returns the
    ///     result of the first computation that completes and cancels the other.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<|>) (left : Cloud<'a>) (right : Cloud<'a>) : Cloud<'a> =
        cloud {
            let! result = 
                Cloud.Choice [| cloud { let! value = left  in return Some (value) }
                                cloud { let! value = right in return Some (value) }  |]

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
    let inline (<--) (channel : ISendPort<'T>) (message : 'T) : Cloud<unit> = channel.Send message