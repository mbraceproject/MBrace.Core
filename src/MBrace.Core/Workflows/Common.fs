namespace MBrace.Workflows

open MBrace

[<RequireQualifiedAccess>]
module Cloud =

    /// <summary>
    ///     Embeds a value in cloud workflow.
    /// </summary>
    /// <param name="t">Value to be embedded.</param>
    let ret (t : 'T) : Cloud<'T> = cloud { return t }

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift (f : 'T -> 'S) (t : 'T) : Cloud<'S> = cloud { return f t }

    /// <summary>
    ///     Lifts function to cloud workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift2 (f : 'T1 -> 'T2 -> 'S) (t1 : 'T1) (t2 : 'T2) : Cloud<'S> = cloud { return f t1 t2 }

    /// <summary>
    ///     Cloud workflow map combinator.
    /// </summary>
    /// <param name="mapper">Mapping function.</param>
    /// <param name="tworkflow">Input workflow.</param>
    let map (mapper : 'T -> 'S) (tworkflow : Cloud<'T>) = cloud { let! t = tworkflow in return mapper t }

[<RequireQualifiedAccess>]
module Local =

    /// <summary>
    ///     Embeds a value in local workflow.
    /// </summary>
    /// <param name="t">Value to be embedded.</param>
    let ret (t : 'T) : Local<'T> = local { return t }

    /// <summary>
    ///     Lifts function to local workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift (f : 'T -> 'S) (t : 'T) : Local<'S> = local { return f t }

    /// <summary>
    ///     Lifts function to local workflow.
    /// </summary>
    /// <param name="f">Input function</param>
    let lift2 (f : 'T1 -> 'T2 -> 'S) (t1 : 'T1) (t2 : 'T2) : Local<'S> = local { return f t1 t2 }

    /// <summary>
    ///     Local workflow map combinator.
    /// </summary>
    /// <param name="mapper">Mapping function.</param>
    /// <param name="tworkflow">Input workflow.</param>
    let map (mapper : 'T -> 'S) (tworkflow : Local<'T>) = local { let! t = tworkflow in return mapper t }


/// collection of parallelism operators for the cloud
[<AutoOpen>]
module CloudOperators =
        
    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
        Cloud.Parallel(left, right)

    /// <summary>
    ///     Combines two cloud computations into one that executes them in parallel and returns the
    ///     result of the first computation that completes and cancels the other.
    /// </summary>
    /// <param name="left">The first cloud computation.</param>
    /// <param name="right">The second cloud computation.</param>
    let (<|>) (left : Cloud<'a>) (right : Cloud<'a>) : Cloud<'a> =
        cloud {
            let left' = cloud { let! value = left  in return Some (value) }
            let right'= cloud { let! value = right in return Some (value) }
            let! result = Cloud.Choice [| left' ; right' |]
            return result.Value
        }

    /// <summary>
    ///     Send a message to cloud channel.
    /// </summary>
    /// <param name="channel">Target channel.</param>
    /// <param name="msg">Input message.</param>
    let inline (<--) (channel : ISendPort<'T>) (message : 'T) : Local<unit> = local { return! channel.Send message }

    /// <summary>
    ///     Awaits a message from cloud channel.
    /// </summary>
    /// <param name="channel">Source channel.</param>
    let inline (?) (channel : IReceivePort<'T>) : Local<'T> = local { return! channel.Receive() }