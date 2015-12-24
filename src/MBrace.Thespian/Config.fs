namespace MBrace.Thespian.Runtime

open System
open System.IO
open System.Net
open System.Reflection
open System.Threading

open Nessos.Vagabond

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.DebugUtils

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store

/// MBrace.Thespian configuration object
type Config private () =

    static let _isInitialized = ref false
    static let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
    static let mutable _objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable _localFileStore = Unchecked.defaultof<FileSystemStore>
    static let mutable _workingDirectory = Unchecked.defaultof<string>
    static let mutable _localTcpPort = Unchecked.defaultof<int>

    static let checkInitialized () =
        if not _isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    /// <summary>
    ///     Initializes the global configuration object.
    /// </summary>
    static member Initialize(populateDirs : bool, isClient : bool, ?hostname : string, ?workingDirectory : string, ?port : int) =
        lock _isInitialized (fun () ->
            if _isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
            _workingDirectory <- match workingDirectory with Some wd -> wd | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess(prefix = "mbrace.thespian")
            let _ = WorkingDirectory.CreateWorkingDirectory(_workingDirectory, cleanup = populateDirs)
            let vagabondDir = Path.Combine(_workingDirectory, "vagabond")
            if populateDirs then ignore <| Directory.CreateDirectory vagabondDir
            VagabondRegistry.Initialize(vagabondDir, isClientSession = isClient, forceLocalFSharpCore = true)

            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)
            ServicePointManager.DefaultConnectionLimit <- 12 * Environment.ProcessorCount
            _objectCache <- InMemoryCache.Create()

            let fsStoreDirectory = Path.Combine(_workingDirectory, "store")
            _localFileStore <- FileSystemStore.Create(fsStoreDirectory, create = true, cleanup = populateDirs)

            // Thespian initialization
            let thespianSerializer = new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)
//            let thespianSerializer = new DebugSerializer(thespianSerializer) // use for debug information on serialized values
            Nessos.Thespian.Serialization.defaultSerializer <- thespianSerializer
            Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite

            hostname |> Option.iter (fun h -> TcpListenerPool.DefaultHostname <- h ; ignore TcpListenerPool.IPAddresses)
            TcpListenerPool.RegisterListener(defaultArg port 0)
            let listeners = TcpListenerPool.GetListeners(IPEndPoint.any) |> Seq.toArray
            if listeners.Length <> 1 then
                raise <| new InvalidOperationException("FATAL: unexpected number of registered Thespian TCP listeners.")

            _localTcpPort <- listeners.[0].LocalEndPoint.Port
            _isInitialized := true)

    /// True if running on mono
    static member RunsOnMono = runsOnMono
    /// FsPickler serializer instance used by MBrace.Thespian
    static member Serializer = checkInitialized() ; VagabondRegistry.Instance.Serializer
    /// Working directory used by the instance
    static member WorkingDirectory = checkInitialized() ; _workingDirectory
    /// Local FileSystemStore instance
    static member FileSystemStore = checkInitialized() ; _localFileStore
    /// Object Cache used by the instance
    static member ObjectCache = checkInitialized() ; _objectCache
    /// TCP Port used by the local Thespian instance
    static member LocalTcpPort = checkInitialized() ; _localTcpPort
    /// Local TCP address used by the local Thespian instance
    static member LocalAddress = checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname _localTcpPort
    /// Local MBrace uri identifying the current worker instance
    static member LocalMBraceUri = checkInitialized() ; sprintf "mbrace://%s:%d" TcpListenerPool.DefaultHostname _localTcpPort
    /// Hostname that the thespian instance is listening to
    static member HostName = checkInitialized() ; TcpListenerPool.DefaultHostname
    /// Sets the System.Console title
    static member SetConsoleTitle(?status : string) =
        checkInitialized()
        let pid = System.Diagnostics.Process.GetCurrentProcess().Id
        let status = defaultArg status "Worker"
        Console.Title <- sprintf "MBrace.Thespian %s [pid:%d, port:%d]" status pid Config.LocalTcpPort