namespace MBrace.Runtime

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

/// A logger that writes to the system console
type ConsoleLogger () =
    interface ICloudLogger with
        member __.Log(message : string) = System.Console.WriteLine message

/// A logger that performs no action
type NullLogger () =
    interface ICloudLogger with
        member __.Log _ = ()