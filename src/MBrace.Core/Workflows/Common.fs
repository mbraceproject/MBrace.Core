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