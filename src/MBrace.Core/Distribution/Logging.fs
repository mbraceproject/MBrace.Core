namespace MBrace.Core.Internals

open System

/// <summary>
///     Cloud Workflow logger.
/// </summary>
type ICloudLogger =
    /// <summary>
    ///     Log a new message to the execution context.
    /// </summary>
    /// <param name="entry">Entry to be logged.</param>
    abstract Log : entry:string -> unit