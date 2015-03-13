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