namespace MBrace.Runtime

open System
open System.IO
open System.Collections.Generic
open System.Runtime.Serialization
open System.Threading

open Microsoft.FSharp.Control

open MBrace.Core.Internals
open MBrace.Runtime.Utils

/// LogLevel enumeration
type LogLevel =
    /// Indicates logs at all levels.
    | Undefined = 0
    /// Indicates logs for a critical alert.
    | Critical  = 1
    /// Indicates logs for an error.
    | Error     = 2
    /// Indicates logs for a warning.
    | Warning   = 3
    /// Indicates logs for an informational message.
    | Info      = 4
    /// Indicates logs for debugging.
    | Debug     = 5


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module LogLevel =
    
    /// Pretty-prints supplied log level
    let inline print (level : LogLevel) : string =
        match level with
        | LogLevel.Critical -> "CRITICAL"
        | LogLevel.Error -> "ERROR"
        | LogLevel.Warning -> "WARNING"
        | LogLevel.Info -> "INFO"
        | LogLevel.Debug -> "DEBUG"
        | _ -> ""

/// Struct that specifies a single system log entry
[<Struct; DataContract>]
type SystemLogEntry =
    [<DataMember(Name = "LogLevel", Order = 0)>]
    val private logLevel : LogLevel
    [<DataMember(Name = "DateTime", Order = 1)>]
    val private dateTime : DateTimeOffset
    [<DataMember(Name = "Message", Order = 2)>]
    val private message : string

    /// Creates a new log entry with supplied parameters.
    new (logLevel : LogLevel, message : string, dateTime : DateTimeOffset) =
        { dateTime = dateTime ; logLevel = logLevel ; message = message }

    /// Creates a new log entry with datetime set to current machine date.
    new (logLevel : LogLevel, message : string) =
        { dateTime = DateTimeOffset.Now ; logLevel = logLevel ; message = message }

    /// Date of log entry
    member __.DateTime = __.dateTime
    /// LogLevel of log entry
    member __.LogLevel = __.logLevel
    /// Logged message of entry
    member __.Message = __.message

    /// <summary>
    ///     Displays log entry as string with supplied parameters.
    /// </summary>
    /// <param name="entry">Entry to be displayed.</param>
    /// <param name="showDate">Display date at the beggining of the log entry. Defaults to true.</param>
    static member Format (entry : SystemLogEntry, ?showDate : bool) =
        if defaultArg showDate true then
            let local = entry.dateTime.LocalDateTime
            let fmt = local.ToString("yyyy-MM-dd H:mm:ss")
            sprintf "[%s] %O : %s" fmt (LogLevel.print entry.logLevel) entry.message
        else 
            sprintf "%O : %s" (LogLevel.print entry.logLevel) entry.message

/// Abstract logger type used by underlying MBrace runtime implementations.
type ISystemLogger =

    /// <summary>
    ///     Logs a new entry.
    /// </summary>
    /// <param name="entry">Entry to be logged.</param>
    abstract LogEntry : entry:SystemLogEntry -> unit

[<RequireQualifiedAccess>]
module Logger =

    /// Logs a new entry with provided level using the current date time
    let inline log (logger : ISystemLogger) (level : LogLevel) (message : string) = logger.LogEntry(new SystemLogEntry(level, message))
    /// Logs a new entry with provided level and formatted message
    let inline logF (logger : ISystemLogger) (level : LogLevel) fmt = Printf.ksprintf (log logger level) fmt

    /// Logs information message with current time
    let inline logInfo (logger : ISystemLogger) (message : string) = log logger LogLevel.Info message
    /// Logs warning message with current time
    let inline logWarning (logger : ISystemLogger) (message : string) = log logger LogLevel.Warning message
    /// Logs error message with current time
    let inline logError (logger : ISystemLogger) (message : string) = log logger LogLevel.Error message
    /// Logs critical error message with current time
    let inline logCritical (logger : ISystemLogger) (message : string) = log logger LogLevel.Critical message

    /// Logs a new entry with provided level and exception using the current date time
    let inline logWithException (logger : ISystemLogger) (level : LogLevel) (exn : exn) (message : string) = 
        let message = sprintf "%s:\nException=%O" message  exn
        log logger level message


    /// <summary>
    ///     Filters log entries that reach the target logger according
    ///     to supplied predicate.
    /// </summary>
    /// <param name="filterF">LogEntry filter predicate.</param>
    /// <param name="target">Target logger.</param>
    let filter (filterF : SystemLogEntry -> bool) (target : ISystemLogger) =
        { new ISystemLogger with
            member __.LogEntry(e : SystemLogEntry) =
                if filterF e then target.LogEntry e
        }

    /// <summary>
    ///     Filters log entries that reach the target logger according
    ///     to supplied log level.
    /// </summary>
    /// <param name="maxLogLevel">Only forward log level that are less than or equal to supplied level.</param>
    /// <param name="target">Target logger.</param>
    let filterLogLevel (maxLogLevel : LogLevel) (target : ISystemLogger) =
        { new ISystemLogger with
            member __.LogEntry(e : SystemLogEntry) =
                if e.LogLevel <= maxLogLevel then target.LogEntry e
        }

[<AutoOpen>]
module LogUtils =

    type ISystemLogger with
        /// Logs a new entry with provided level using the current date time
        member l.Log (level : LogLevel) (message : string) = Logger.log l level message
        /// Logs a new entry with provided level and formatted message
        member l.Logf level fmt = Logger.logF l level fmt
        /// Logs information message with current time
        member l.LogInfo message = Logger.logInfo l message
        /// Logs warning message with current time
        member l.LogWarning message = Logger.logWarning l message
        /// Logs error message with current time
        member l.LogError message = Logger.logError l message
        /// Logs critical error message with current time
        member l.LogCritical message = Logger.logCritical l message
        /// Logs a new entry with provided level and exception using the current date time
        member l.LogWithException level exn txt = Logger.logWithException l level exn txt


/// A logger that performs no action
type NullLogger () =
    interface ISystemLogger with
        member __.LogEntry _ = ()

/// A logger that writes to the system console
type ConsoleLogger (?showDate : bool, ?useColors : bool) =
    let showDate = defaultArg showDate false
    let useColors = defaultArg useColors false
    interface ISystemLogger with
        member __.LogEntry(e) =
            let text = SystemLogEntry.Format(e, showDate)
            if useColors then
                let currentColor = Console.ForegroundColor
                Console.ForegroundColor <-
                    match e.LogLevel with
                    | LogLevel.Critical     -> ConsoleColor.Red
                    | LogLevel.Error        -> ConsoleColor.Red
                    | LogLevel.Warning      -> ConsoleColor.Yellow
                    | LogLevel.Info         -> ConsoleColor.Cyan
                    | LogLevel.Debug        -> ConsoleColor.White
                    | LogLevel.Undefined    -> ConsoleColor.Gray
                    | _                     -> currentColor

                Console.WriteLine text
                Console.ForegroundColor <- currentColor
            else
                Console.WriteLine text

/// Logger that writes log entries to a local file
[<AutoSerializable(false)>]
type FileSystemLogger private (fs : FileStream, writer : StreamWriter, showDate : bool) =

    /// <summary>
    ///     Creates a new file logger instance.
    /// </summary>
    /// <param name="path">Path to log file.</param>
    /// <param name="showDate">Show date as a prefix to log entries. Defaults to true.</param>
    /// <param name="append">Append entries if file already exists. Defaults to true.</param>
    static member Create(path : string, ?showDate : bool, ?append : bool) =
        let showDate = defaultArg showDate true
        let fileMode = if defaultArg append true then FileMode.OpenOrCreate else FileMode.Create
        let fs = new FileStream(path, fileMode, FileAccess.Write, FileShare.Read)
        let writer = new StreamWriter(fs)
        new FileSystemLogger(fs, writer, showDate)

    interface ISystemLogger with
        member __.LogEntry(e : SystemLogEntry) = 
            let text = SystemLogEntry.Format(e, showDate = showDate)
            writer.WriteLine text
             
    interface IDisposable with
        member __.Dispose () = writer.Flush () ; writer.Close () ; fs.Close()

/// Logger that can be used to subscribe underlying loggers.
[<Sealed; AutoSerializable(false)>]
type AttacheableLogger private (logLevel : LogLevel, useAsync : bool) =

    let mutable logLevel = logLevel
    let mutable attachedLoggers = Array.empty<ISystemLogger> 
    let mutable loggerIds = Array.empty<string>

    let logEntry (e : SystemLogEntry) =
        if e.LogLevel <= logLevel then
            for l in attachedLoggers do l.LogEntry e

    let rec loop (inbox : MailboxProcessor<SystemLogEntry>) = async {
        let! e = inbox.Receive()
        do logEntry e
        return! loop inbox
    }

    let behaviour (inbox : MailboxProcessor<SystemLogEntry>) = async {
        try return! loop inbox
        with e -> eprintfn "AsyncLogger has failed with exception:\n%O" e
    }

    let mutable cts = Unchecked.defaultof<_>
    let mutable actor = Unchecked.defaultof<_>
    do if useAsync then 
        cts <- new CancellationTokenSource()
        actor <- MailboxProcessor.Start(behaviour, cts.Token)

    let attach (l : ISystemLogger) =
        lock attachedLoggers (fun () ->
            let id = mkUUID()
            loggerIds <- Array.append loggerIds [|id|]
            attachedLoggers <- Array.append attachedLoggers [|l|]
            id)

    let detach(id : string) =
        lock attachedLoggers (fun () ->
            match loggerIds |> Array.tryFindIndex ((=) id) with
            | None -> ()
            | Some i ->
                loggerIds <- loggerIds |> Array.filteri (fun i' _ -> i <> i')
                attachedLoggers <- attachedLoggers |> Array.filteri (fun i' _ -> i <> i'))

    /// <summary>
    ///     Creates a new attachedable logger instance.
    /// </summary>
    /// <param name="logLevel">Log level to be forwarded to subscribed workers. Defaults to maximum log level.</param>
    /// <param name="makeAsynchronous">Specifies if log entries should be pushed asynchronously to subscribed loggers. Defaults to false.</param>
    static member Create(?logLevel : LogLevel, ?makeAsynchronous : bool) =
        let maxLogLevel = defaultArg logLevel (enum Int32.MaxValue)
        let makeAsynchronous = defaultArg makeAsynchronous false
        new AttacheableLogger(maxLogLevel, makeAsynchronous)

    /// Gets or sets the maximum log level to be forwarded to subscribed loggers
    member __.LogLevel
        with get () = logLevel
        and set l = logLevel <- l

    /// <summary>
    ///     Subscribes a logger instance to the attacheable logger.
    ///     Returns an unsubscribe disposable.
    /// </summary>
    /// <param name="logger">Logger to be subscribed.</param>
    member __.AttachLogger(logger : ISystemLogger) : IDisposable =
        let id = attach logger
        let isDisposed = ref false
        { new IDisposable with member __.Dispose() = if not !isDisposed then detach id }

    interface ISystemLogger with
        member __.LogEntry e =
            if useAsync then
                actor.Post e
            else 
                logEntry e

    interface IDisposable with
        member __.Dispose() = if useAsync then cts.Cancel()

type private LoggerProxy(logger : ISystemLogger) =
    inherit MarshalByRefObject()
    override __.InitializeLifetimeService() = null
    member __.LogEntry(entry : SystemLogEntry) = logger.LogEntry entry

/// Serializable Logger proxy implementation that can be marshaled across AppDomains
[<Sealed; DataContract>]
type MarshaledLogger(logger : ISystemLogger) =
    [<IgnoreDataMember>]
    let mutable proxy = new LoggerProxy(logger)
    [<DataMember(Name = "ObjRef")>]
    let objRef = System.Runtime.Remoting.RemotingServices.Marshal(proxy)
    [<OnDeserialized>]
    let _onDeserialized (_ : StreamingContext) =
        proxy <- System.Runtime.Remoting.RemotingServices.Unmarshal objRef :?> LoggerProxy

    interface ISystemLogger with
        member __.LogEntry(entry : SystemLogEntry) = proxy.LogEntry entry