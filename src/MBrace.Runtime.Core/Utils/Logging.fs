namespace MBrace.Runtime

open System
open System.IO
open System.Collections.Generic

open MBrace.Core.Internals

/// LogLevel enumeration
type LogLevel =
    | None      = 0
    | Error     = 1
    | Warning   = 2
    | Info      = 3
    | Debug     = 4

type ISystemLogger =

    /// <summary>
    ///     Logs a new entry.
    /// </summary>
    /// <param name="level">Log level for entry.</param>
    /// <param name="time">Time of logged entry.</param>
    /// <param name="message">Message to be logged.</param>
    abstract LogEntry : level:LogLevel * time:DateTime * message:string -> unit

[<AutoOpen>]
module private LoggerImpl =
    
    let inline levelToString (level : LogLevel) =
        match level with
        | LogLevel.Info -> "INFO"
        | LogLevel.Warning -> "WARNING"
        | LogLevel.Error -> "ERROR"
        | LogLevel.Debug -> "DEBUG"
        | _ -> ""

    let inline entryToString showDate (level : LogLevel) (time : DateTime) (message : string) =
        if showDate then
            let fmt = time.ToString("yyyy-MM-dd H:mm:ss")
            sprintf "[%s] %O : %s" fmt (levelToString level) message
        else 
            sprintf "%O : %s" (levelToString level) message
        

/// A logger that writes to the system console
type ConsoleSystemLogger (?showDate : bool) =
    let showDate = defaultArg showDate false
    interface ISystemLogger with
        member __.LogEntry(level : LogLevel, time:DateTime, message : string) =
            let text = entryToString showDate level time message
            Console.WriteLine(text)

/// A logger that performs no action
type NullSystemLogger () =
    interface ISystemLogger with
        member __.LogEntry (_,_,_) = ()

/// Writes logs to local file
[<AutoSerializable(false)>]
type FileSystemLogger (path : string, ?showDate : bool, ?append : bool) =

    let showDate = defaultArg showDate true
    let fileMode = if defaultArg append true then FileMode.OpenOrCreate else FileMode.Create
    let fs = new FileStream(path, fileMode, FileAccess.Write, FileShare.Read)
    let writer = new StreamWriter(fs)

    interface ISystemLogger with
        member __.LogEntry(level : LogLevel, time : DateTime, message : string) = 
            let text = entryToString showDate level time message
            writer.WriteLine text
             
    interface IDisposable with
        member __.Dispose () = writer.Flush () ; writer.Close () ; fs.Close()


type AttacheableLogger() =
    let attached = new Dictionary<string, ISystemLogger>()
    member __.AttachLogger(logger : ISystemLogger) =
        let id = mkUUID()
        lock attached (fun () -> attached.Add(id, logger))
        { new IDisposable with member __.Dispose() = lock attached (fun () -> ignore <| attached.Remove id) }
        
    interface ISystemLogger with
        member __.LogEntry(level : LogLevel, time : DateTime, message : string) = 
            for kv in attached do kv.Value.LogEntry(level, time, message)


[<RequireQualifiedAccess>]
module Logger =

    let inline log (l : ISystemLogger) lvl txt = l.LogEntry(lvl, DateTime.Now, txt)
    let inline logWithException (l : ISystemLogger) lvl (exn : exn) txt = 
        let message = sprintf "%s:\nException=%O" txt  exn
        log l lvl message

    let inline logF (l : ISystemLogger) lvl fmt = Printf.ksprintf (log l lvl) fmt
    let inline logInfo (l : ISystemLogger) txt = log l LogLevel.Info txt
    let inline logError (l : ISystemLogger) txt = log l LogLevel.Error txt
    let inline logWarning (l : ISystemLogger) txt = log l LogLevel.Warning txt

[<AutoOpen>]
module LogUtils =

    type ISystemLogger with
        member l.Log (lvl : LogLevel) (text : string) = Logger.log l lvl text
        member l.LogWithException exn text lvl = Logger.logWithException l lvl exn text
        member l.Logf lvl fmt = Logger.logF l lvl fmt
        member l.LogInfo text = Logger.logInfo l text
        member l.LogError text = Logger.logError l text
        member l.LogWithException lvl exn txt = Logger.logWithException l lvl exn txt
        member l.LogWarning text = Logger.logWarning l text