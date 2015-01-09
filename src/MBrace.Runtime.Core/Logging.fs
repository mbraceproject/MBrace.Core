namespace MBrace.Runtime.Logging

open MBrace.Continuation

/// A logger that writes to the system console
type ConsoleLogger () =
    interface ICloudLogger with
        member __.Log(message : string) = System.Console.WriteLine message

/// A logger that ignores entries
type NullLogger () =
    interface ICloudLogger with
        member __.Log _ = ()